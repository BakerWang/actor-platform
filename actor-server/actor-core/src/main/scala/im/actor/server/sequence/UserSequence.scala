package im.actor.server.sequence

import akka.actor._
import akka.pattern.pipe
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.protobuf.ByteString
import com.google.protobuf.wrappers.{ Int32Value, StringValue }
import im.actor.server.db.DbExtension
import im.actor.server.model.{ SeqUpdate, UpdateMapping }
import im.actor.server.persist.sequence.UserSequenceRepo
import im.actor.server.pubsub.PubSubExtension

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{ Failure, Success }

object UserSequence {
  def topic(userId: Int): String = s"sequence.$userId"

  private final case class Initialized(commonSeq: Int, seqs: Map[Long, Int])

  private[sequence] def props =
    Props(new UserSequence)
}

private trait SeqControl {
  private var _commonSeq: Int = 0

  protected def commonSeq = this._commonSeq
  protected def commonSeq_=(seq: Int) = this._commonSeq = seq

  protected def nextCommonSeq(): Int = {
    val nseq = this._commonSeq + 1
    this._commonSeq = nseq
    nseq
  }

  private var _seqMap: Map[Long, Int] = Map.empty

  protected def seqMap = this._seqMap
  protected def seqMap_=(map: Map[Long, Int]) = this._seqMap = map

  protected def nextSeq(authId: Long) = {
    val nextSeq = _seqMap.getOrElse(authId, 0)
    this._seqMap += (authId -> nextSeq)
    nextSeq
  }
}

private[sequence] final class UserSequence extends Actor with ActorLogging with Stash with SeqControl {

  import UserSequence._
  import UserSequenceCommands._
  import akka.cluster.pubsub.DistributedPubSubMediator._
  import context.dispatcher

  private val db = DbExtension(context.system).db
  private val connector = DbExtension(context.system).connector
  private val seqUpdExt = SeqUpdatesExtension(context.system)
  private val pubSubExt = PubSubExtension(context.system)

  val userId = self.path.name.toInt

  private val deliveryCache = Caffeine.newBuilder().maximumSize(100).executor(context.dispatcher).build[String, SeqState]()

  private lazy val vendorPush = context.actorOf(VendorPush.props(userId), "vendor-push")

  init()

  def receive = {
    case Initialized(commonSeq, seqs) ⇒
      this.commonSeq = commonSeq
      this.seqMap = seqs
      unstashAll()
      context become initialized
    case Status.Failure(e) ⇒
      log.error(e, "Failed to initialize UserSequence")
      init()
    case msg ⇒ stash()
  }

  def initialized: Receive = {
    case cmd: VendorPushCommand ⇒ vendorPush forward cmd
    case DeliverUpdate(mappingOpt, pushRules, reduceKey, deliveryId) ⇒
      mappingOpt match {
        case Some(mapping) ⇒ deliver(mapping, pushRules, reduceKey, deliveryId)
        case None ⇒ log.error("Empty mapping")
      }
    case GetSeqState(authId) ⇒
      val seq = seqMap getOrElse (authId, 0)
      sender() ! SeqState(seq)
  }

  private def init(): Unit =
    (for {
      seqs <- connector.run(SeqStorage.getPairs(Some(userId.toString))) map (_.toMap map { case (key, value) => key.toLong -> Int32Value.parseFrom(value).value })
      commonSeq ← db.run(UserSequenceRepo.fetchSeq(userId) map (_ getOrElse 0)
    } yield Initialized(commonSeq, seqs)) pipeTo self

  private def deliver(mapping: UpdateMapping, pushRules: Option[PushRules], reduceKey: Option[StringValue], deliveryId: String): Unit = {
    cached(deliveryId) {
      val seq = nextSeq()

      val seqUpdate = SeqUpdate(
        userId,
        seq,
        System.currentTimeMillis(),
        reduceKey,
        Some(mapping)
      )

      writeToDb(seqUpdate) map (_ ⇒ SeqState(seq)) andThen {
        case Success(_) ⇒
          pubSubExt.publish(Publish(topic(userId), UserSequenceEvents.NewUpdate(Some(seqUpdate), pushRules, None, ByteString.EMPTY)))
          vendorPush ! DeliverPush(seq, pushRules)
      }
    }
  }

  private def cached(deliveryId: String)(f: ⇒ Future[SeqState]): Unit = {
    (if (deliveryId.nonEmpty) {
      Option(deliveryCache.getIfPresent(deliveryId)) match {
        case Some(seqstate) ⇒ Future.successful(seqstate)
        case None           ⇒ f
      }
    } else f) pipeTo sender() onComplete {
      case Success(s) ⇒ deliveryCache.put(deliveryId, s)
      case Failure(e) ⇒ log.error(e, "Failed to deliver")
    }
  }

  private def writeToDb(seqUpdate: SeqUpdate): Future[Unit] = seqUpdExt.persistUpdate(seqUpdate)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.error(reason, "Failure while processing {}", message)
  }
}
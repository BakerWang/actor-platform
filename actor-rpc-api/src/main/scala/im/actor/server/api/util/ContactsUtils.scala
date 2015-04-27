package im.actor.server.api.util

import scala.concurrent.ExecutionContext

import slick.dbio.Effect.{ Read, All, Write }
import slick.dbio.{ DBIO, NoStream, DBIOAction }

import im.actor.api.rpc.AuthorizedClientData
import im.actor.api.rpc.contacts.UpdateContactsAdded
import im.actor.server.models
import im.actor.server.persist
import im.actor.server.push.{ SeqUpdatesManager, SeqUpdatesManagerRegion }

object ContactsUtils {

  import SeqUpdatesManager._

  def addContactSendUpdate(
    userId:      Int,
    phoneNumber: Long,
    name:        Option[String],
    accessSalt:  String
  )(implicit
    client: AuthorizedClientData,
    ec:                  ExecutionContext,
    seqUpdManagerRegion: SeqUpdatesManagerRegion): DBIOAction[(Sequence, Array[Byte]), NoStream, Write with All] = {
    for {
      _ ← persist.contact.UserContact.createOrRestore(client.userId, userId, phoneNumber, name, accessSalt)
      seqstate ← broadcastClientUpdate(UpdateContactsAdded(Vector(userId)), None)
    } yield seqstate
  }

  def getLocalNameOrDefault(ownerUserId: Int, contactUser: models.User)(implicit ec: ExecutionContext): DBIOAction[String, NoStream, Read] = {
    persist.contact.UserContact.findName(ownerUserId, contactUser.id).headOption map {
      case Some(Some(name)) ⇒ name
      case _                ⇒ contactUser.name
    }
  }
}

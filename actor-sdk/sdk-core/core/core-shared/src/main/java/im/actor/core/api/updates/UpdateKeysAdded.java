package im.actor.core.api.updates;
/*
 *  Generated by the Actor API Scheme generator.  DO NOT EDIT!
 */

import im.actor.runtime.bser.*;
import im.actor.runtime.collections.*;
import static im.actor.runtime.bser.Utils.*;
import im.actor.core.network.parser.*;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.NotNull;
import com.google.j2objc.annotations.ObjectiveCName;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import im.actor.core.api.*;

public class UpdateKeysAdded extends Update {

    public static final int HEADER = 0x70;
    public static UpdateKeysAdded fromBytes(byte[] data) throws IOException {
        return Bser.parse(new UpdateKeysAdded(), data);
    }

    private int uid;
    private int keyGroupId;
    private List<ApiEncryptionKey> keys;
    private List<ApiEncryptionKeySignature> signatures;

    public UpdateKeysAdded(int uid, int keyGroupId, @NotNull List<ApiEncryptionKey> keys, @NotNull List<ApiEncryptionKeySignature> signatures) {
        this.uid = uid;
        this.keyGroupId = keyGroupId;
        this.keys = keys;
        this.signatures = signatures;
    }

    public UpdateKeysAdded() {

    }

    public int getUid() {
        return this.uid;
    }

    public int getKeyGroupId() {
        return this.keyGroupId;
    }

    @NotNull
    public List<ApiEncryptionKey> getKeys() {
        return this.keys;
    }

    @NotNull
    public List<ApiEncryptionKeySignature> getSignatures() {
        return this.signatures;
    }

    @Override
    public void parse(BserValues values) throws IOException {
        this.uid = values.getInt(1);
        this.keyGroupId = values.getInt(2);
        List<ApiEncryptionKey> _keys = new ArrayList<ApiEncryptionKey>();
        for (int i = 0; i < values.getRepeatedCount(3); i ++) {
            _keys.add(new ApiEncryptionKey());
        }
        this.keys = values.getRepeatedObj(3, _keys);
        List<ApiEncryptionKeySignature> _signatures = new ArrayList<ApiEncryptionKeySignature>();
        for (int i = 0; i < values.getRepeatedCount(4); i ++) {
            _signatures.add(new ApiEncryptionKeySignature());
        }
        this.signatures = values.getRepeatedObj(4, _signatures);
    }

    @Override
    public void serialize(BserWriter writer) throws IOException {
        writer.writeInt(1, this.uid);
        writer.writeInt(2, this.keyGroupId);
        writer.writeRepeatedObj(3, this.keys);
        writer.writeRepeatedObj(4, this.signatures);
    }

    @Override
    public String toString() {
        String res = "update KeysAdded{";
        res += "uid=" + this.uid;
        res += ", keyGroupId=" + this.keyGroupId;
        res += ", keys=" + this.keys.size();
        res += ", signatures=" + this.signatures.size();
        res += "}";
        return res;
    }

    @Override
    public int getHeaderKey() {
        return HEADER;
    }
}

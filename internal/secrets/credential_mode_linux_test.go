//go:build linux

package secrets

import (
	"encoding/binary"
	"testing"
)

func TestACLGrantsOnlyNamedServiceUser(t *testing.T) {
	const serviceUID = 991
	valid := encodeACL([]aclTestEntry{
		{aclUserObject, aclRead, aclUndefinedID},
		{aclUser, aclRead, serviceUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, aclRead, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	})
	if !aclGrantsOnlyServiceUser(valid, serviceUID) {
		t.Fatal("systemd service-user ACL was rejected")
	}
	for name, entries := range map[string][]aclTestEntry{
		"shared group": {
			{aclUserObject, aclRead, aclUndefinedID}, {aclUser, aclRead, serviceUID},
			{aclGroupObject, aclRead, aclUndefinedID}, {aclMask, aclRead, aclUndefinedID}, {aclOther, 0, aclUndefinedID},
		},
		"extra user": {
			{aclUserObject, aclRead, aclUndefinedID}, {aclUser, aclRead, serviceUID}, {aclUser, aclRead, 992},
			{aclGroupObject, 0, aclUndefinedID}, {aclMask, aclRead, aclUndefinedID}, {aclOther, 0, aclUndefinedID},
		},
		"writable service user": {
			{aclUserObject, aclRead, aclUndefinedID}, {aclUser, 0x06, serviceUID},
			{aclGroupObject, 0, aclUndefinedID}, {aclMask, 0x06, aclUndefinedID}, {aclOther, 0, aclUndefinedID},
		},
	} {
		if aclGrantsOnlyServiceUser(encodeACL(entries), serviceUID) {
			t.Fatalf("unsafe ACL %q was accepted", name)
		}
	}
}

type aclTestEntry struct {
	tag, permissions uint16
	id               uint32
}

func encodeACL(entries []aclTestEntry) []byte {
	encoded := make([]byte, 4+8*len(entries))
	binary.NativeEndian.PutUint32(encoded[:4], posixACLVersion)
	for index, entry := range entries {
		offset := 4 + index*8
		binary.NativeEndian.PutUint16(encoded[offset:offset+2], entry.tag)
		binary.NativeEndian.PutUint16(encoded[offset+2:offset+4], entry.permissions)
		binary.NativeEndian.PutUint32(encoded[offset+4:offset+8], entry.id)
	}
	return encoded
}

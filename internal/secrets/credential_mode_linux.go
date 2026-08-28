//go:build linux

package secrets

import (
	"encoding/binary"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	posixACLVersion = 2
	aclUserObject   = 0x01
	aclUser         = 0x02
	aclGroupObject  = 0x04
	aclGroup        = 0x08
	aclMask         = 0x10
	aclOther        = 0x20
	aclRead         = 0x04
	aclUndefinedID  = ^uint32(0)
)

func privateCredentialFile(path string, info os.FileInfo) bool {
	if info.Mode().Perm() == 0o400 {
		return true
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode().Perm() != 0o440 || stat.Uid != 0 || os.Geteuid() == 0 {
		return false
	}
	size, err := unix.Getxattr(path, "system.posix_acl_access", nil)
	if err != nil || size <= 0 || size > 4+8*16 {
		return false
	}
	acl := make([]byte, size)
	read, err := unix.Getxattr(path, "system.posix_acl_access", acl)
	if err != nil || read != size {
		return false
	}
	return aclGrantsOnlyServiceUser(acl, uint32(os.Geteuid()))
}

func aclGrantsOnlyServiceUser(acl []byte, serviceUID uint32) bool {
	if len(acl) < 4 || (len(acl)-4)%8 != 0 || binary.NativeEndian.Uint32(acl[:4]) != posixACLVersion {
		return false
	}
	seen := map[uint16]bool{}
	serviceUser := false
	for offset := 4; offset < len(acl); offset += 8 {
		tag := binary.NativeEndian.Uint16(acl[offset : offset+2])
		permissions := binary.NativeEndian.Uint16(acl[offset+2 : offset+4])
		id := binary.NativeEndian.Uint32(acl[offset+4 : offset+8])
		if permissions&^uint16(0x07) != 0 {
			return false
		}
		switch tag {
		case aclUserObject:
			if seen[tag] || permissions != aclRead || id != aclUndefinedID {
				return false
			}
			seen[tag] = true
		case aclUser:
			if serviceUser || permissions != aclRead || id != serviceUID {
				return false
			}
			serviceUser = true
		case aclGroupObject, aclOther:
			if seen[tag] || permissions != 0 || id != aclUndefinedID {
				return false
			}
			seen[tag] = true
		case aclMask:
			if seen[tag] || permissions != aclRead || id != aclUndefinedID {
				return false
			}
			seen[tag] = true
		case aclGroup:
			return false
		default:
			return false
		}
	}
	return serviceUser && seen[aclUserObject] && seen[aclGroupObject] && seen[aclMask] && seen[aclOther]
}

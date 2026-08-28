//go:build !linux

package secrets

import "os"

func privateCredentialFile(_ string, _ os.FileInfo) bool { return true }

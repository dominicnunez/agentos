package main

import (
	"os"

	"github.com/dominicnunez/agentos/internal/fileguard"
)

func readUnchangedBoundedFile(path string, before os.FileInfo, maximum int64, label string) ([]byte, error) {
	return fileguard.ReadUnchangedBoundedFile(path, before, maximum, label)
}

//go:build linux

package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func readTerminalEscape(input *os.File) ([2]byte, bool, error) {
	var sequence [2]byte
	for index := range sequence {
		ready, err := unix.Poll([]unix.PollFd{{Fd: int32(input.Fd()), Events: unix.POLLIN}}, 100)
		if err != nil {
			return sequence, false, err
		}
		if ready == 0 {
			return sequence, false, nil
		}
		read, err := unix.Read(int(input.Fd()), sequence[index:index+1])
		if err != nil {
			return sequence, false, err
		}
		if read != 1 {
			return sequence, false, nil
		}
	}
	return sequence, true, nil
}

type terminalState unix.Termios

func isTerminal(fd int) bool {
	_, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	return err == nil
}

func makeTerminalRaw(fd int) (*terminalState, error) {
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, err
	}
	raw := *state
	raw.Iflag &^= unix.IGNBRK | unix.BRKINT | unix.PARMRK | unix.ISTRIP | unix.INLCR | unix.IGNCR | unix.ICRNL | unix.IXON
	raw.Oflag &^= unix.OPOST
	raw.Lflag &^= unix.ECHO | unix.ECHONL | unix.ICANON | unix.ISIG | unix.IEXTEN
	raw.Cflag &^= unix.CSIZE | unix.PARENB
	raw.Cflag |= unix.CS8
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return nil, err
	}
	converted := terminalState(*state)
	return &converted, nil
}

func restoreTerminal(fd int, state *terminalState) error {
	converted := unix.Termios(*state)
	return unix.IoctlSetTermios(fd, unix.TCSETS, &converted)
}

func readTerminalSecret(fd int) ([]byte, error) {
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, err
	}
	noEcho := *state
	noEcho.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &noEcho); err != nil {
		return nil, err
	}
	defer func() { _ = unix.IoctlSetTermios(fd, unix.TCSETS, state) }()
	value := make([]byte, 0, 256)
	var one [1]byte
	for len(value) <= 64<<10 {
		read, err := unix.Read(fd, one[:])
		if err != nil {
			return nil, err
		}
		if read == 0 {
			return nil, io.EOF
		}
		if read == 1 {
			value = append(value, one[0])
			if one[0] == '\n' {
				return value, nil
			}
		}
	}
	return nil, fmt.Errorf("secret exceeds 65536 bytes")
}

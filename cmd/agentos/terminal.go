package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

var errSetupExited = fmt.Errorf("setup exited; run agentos to resume")

type terminalUI struct {
	input  *os.File
	output io.Writer
	reader *bufio.Reader
}

func newTerminalUI(input *os.File, output io.Writer) *terminalUI {
	return &terminalUI{input: input, output: output, reader: bufio.NewReader(input)}
}

func (u *terminalUI) selectOne(label string, options []string) (int, error) {
	if len(options) < 2 {
		return 0, fmt.Errorf("selection requires at least two choices")
	}
	fd := int(u.input.Fd())
	if !isTerminal(fd) {
		return 0, fmt.Errorf("interactive setup requires a terminal")
	}
	state, err := makeTerminalRaw(fd)
	if err != nil {
		return 0, err
	}
	defer func() { _ = restoreTerminal(fd, state) }()
	selected := 0
	filter := ""
	visible := filterOptions(options, filter)
	for {
		if len(visible) == 0 {
			selected = 0
		} else if selected >= len(visible) {
			selected = len(visible) - 1
		}
		if err := drawOptions(u.output, label, options, visible, selected, filter); err != nil {
			return 0, err
		}
		key, err := readMenuKey(u.input)
		if err != nil {
			return 0, err
		}
		switch key {
		case "up":
			if len(visible) > 0 {
				selected = (selected - 1 + len(visible)) % len(visible)
			}
		case "down":
			if len(visible) > 0 {
				selected = (selected + 1) % len(visible)
			}
		case "enter":
			if len(visible) > 0 {
				_, _ = fmt.Fprint(u.output, "\r\n")
				return visible[selected], nil
			}
		case "escape":
			_, _ = fmt.Fprint(u.output, "\r\n")
			return 0, errSetupExited
		case "backspace":
			if filter != "" {
				_, size := utf8.DecodeLastRuneInString(filter)
				filter = filter[:len(filter)-size]
				visible = filterOptions(options, filter)
				selected = 0
			}
		default:
			if len(key) == 1 {
				r, _ := utf8.DecodeRuneInString(key)
				if unicode.IsPrint(r) {
					filter += key
					visible = filterOptions(options, filter)
					selected = 0
				}
			}
		}
	}
}

func drawOptions(output io.Writer, label string, options []string, visible []int, selected int, filter string) error {
	if _, err := fmt.Fprintf(output, "\x1b[2J\x1b[H%s\r\n\r\n", safeTerminalLine(label)); err != nil {
		return err
	}
	const maximumVisibleRows = 12
	start := 0
	if selected >= maximumVisibleRows {
		start = selected - maximumVisibleRows + 1
	}
	end := min(start+maximumVisibleRows, len(visible))
	for index := start; index < end; index++ {
		marker := "  "
		if index == selected {
			marker = "> "
		}
		if _, err := fmt.Fprintf(output, "\r\x1b[2K%s%s\r\n", marker, safeTerminalLine(options[visible[index]])); err != nil {
			return err
		}
	}
	if len(visible) == 0 {
		if _, err := fmt.Fprint(output, "\r\x1b[2K  No matching choices\r\n"); err != nil {
			return err
		}
	} else if len(visible) > maximumVisibleRows {
		if _, err := fmt.Fprintf(output, "\r\x1b[2K  Showing %d-%d of %d\r\n", start+1, end, len(visible)); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(output, "\r\x1b[2KUp/Down navigate   Enter select   Esc save and exit   filter: %s\r\n\r\x1b[2K", safeTerminalLine(filter))
	return err
}

func filterOptions(options []string, filter string) []int {
	needle := strings.ToLower(filter)
	visible := make([]int, 0, len(options))
	for index, option := range options {
		if needle == "" || strings.Contains(strings.ToLower(option), needle) {
			visible = append(visible, index)
		}
	}
	return visible
}

func readMenuKey(input *os.File) (string, error) {
	var one [1]byte
	if _, err := io.ReadFull(input, one[:]); err != nil {
		return "", err
	}
	switch one[0] {
	case '\r', '\n':
		return "enter", nil
	case 0x1b:
		sequence, present, err := readTerminalEscape(input)
		if err != nil {
			return "", err
		}
		if !present {
			return "escape", nil
		}
		if sequence[0] == '[' && sequence[1] == 'A' {
			return "up", nil
		}
		if sequence[0] == '[' && sequence[1] == 'B' {
			return "down", nil
		}
		return "escape", nil
	case 0x00, 0xe0:
		if _, err := io.ReadFull(input, one[:]); err != nil {
			return "", err
		}
		if one[0] == 72 {
			return "up", nil
		}
		if one[0] == 80 {
			return "down", nil
		}
	case 0x7f, 0x08:
		return "backspace", nil
	}
	return string(one[:]), nil
}

func (u *terminalUI) line(label string, required bool) (string, error) {
	if _, err := fmt.Fprint(u.output, label+" "); err != nil {
		return "", err
	}
	value, err := u.reader.ReadString('\n')
	if err != nil && !errorsIsEOFWithData(err, value) {
		return "", err
	}
	value = canonicalInput(value)
	if required && value == "" {
		return "", fmt.Errorf("a value is required")
	}
	return value, nil
}

func (u *terminalUI) secret(label string) ([]byte, error) {
	fd := int(u.input.Fd())
	if !isTerminal(fd) {
		return nil, fmt.Errorf("secret entry requires a terminal")
	}
	if _, err := fmt.Fprint(u.output, label+" "); err != nil {
		return nil, err
	}
	value, err := readTerminalSecret(fd)
	_, _ = fmt.Fprintln(u.output)
	if err != nil {
		return nil, err
	}
	value = bytes.TrimRight(value, "\r\n")
	if len(value) == 0 {
		return nil, fmt.Errorf("a value is required")
	}
	return value, nil
}

func errorsIsEOFWithData(err error, value string) bool { return errors.Is(err, io.EOF) && value != "" }

func safeTerminalText(value string) string {
	return strings.Map(func(character rune) rune {
		if character == '\n' || character == '\t' {
			return character
		}
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return -1
		}
		return character
	}, value)
}

func safeTerminalLine(value string) string {
	return strings.Map(func(character rune) rune {
		if character == '\n' || character == '\r' || character == '\t' {
			return ' '
		}
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return -1
		}
		return character
	}, value)
}

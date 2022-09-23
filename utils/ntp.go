package utils

import "time"

type NtpTime uint64

const (
	nanoPerSec        = 1000000000
)

var (
	EpochTime = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
)

func (t NtpTime) DurationSinceEpoch() time.Duration {
	sec := (t >> 32) * nanoPerSec
	frac := (t & 0xffffffff) * nanoPerSec
	nsec := frac >> 32
	if uint32(frac) >= 0x80000000 {
		nsec++
	}
	return time.Duration(sec + nsec)
}

func (t NtpTime) Time() time.Time {
	return EpochTime.Add(t.DurationSinceEpoch())
}

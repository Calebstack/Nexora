package fees

import (
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func dp(s string) *decimal.Decimal { v := d(s); return &v }

func baseSchedule() CorridorSchedule {
	return CorridorSchedule{
		Corridor:      "USDC:NGN",
		SpeedTier:     SpeedStandard,
		Rail:          "NIP",
		Processing:    Charge{Flat: d("0.25"), Bps: d("12.5")},
		RailSurcharge: Charge{Flat: d("0.1"), Bps: d("5")},
		Location:      time.UTC,
		Operations:    1,
	}
}

func TestItemize_HighPrecisionSumsExactly(t *testing.T) {
	in := FeeInput{
		Amount:            d("1234567.1234567"),
		Schedule:          baseSchedule(),
		NetworkFeeStroops: 200,
		XLMRate:           d("0.1234567"),
		At:                time.Date(2026, 10, 19, 3, 0, 0, 0, time.UTC),
		Precision:         7,
	}
	b, err := Itemize(in)
	if err != nil {
		t.Fatalf("Itemize: %v", err)
	}

	// 200 stroops = 0.00002 XLM; * 0.1234567 = 0.000002469134 -> rounded up to 0.0000025.
	// processing: 0.25 + 1234567.1234567*12.5/10000 = 0.25 + 1543.208904320875 = 1543.458904320875 -> 1543.4589044
	// rail:       0.1  + 1234567.1234567*5/10000    = 0.1  + 617.28356172835  = 617.38356172835   -> 617.3835618
	want := map[FeeLineKind]string{
		FeeLineNetwork:    "0.0000025",
		FeeLineProcessing: "1543.4589044",
		FeeLineRail:       "617.3835618",
	}
	sum := decimal.Zero
	for _, l := range b.Lines {
		if !l.Amount.Equal(d(want[l.Kind])) {
			t.Errorf("%s = %s, want %s", l.Kind, l.Amount, want[l.Kind])
		}
		if l.Amount.Exponent() < -7 {
			t.Errorf("%s has more than 7 decimals: %s", l.Kind, l.Amount)
		}
		sum = sum.Add(l.Amount)
	}
	if !b.Total.Equal(sum) || !b.Total.Equal(d("2160.8424687")) {
		t.Fatalf("total = %s, sum(lines) = %s, want 2160.8424687", b.Total, sum)
	}
	if !b.SurgeMultiplier.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("surge = %s, want 1", b.SurgeMultiplier)
	}
}

func TestItemize_RoundsUpNeverDown(t *testing.T) {
	s := baseSchedule()
	s.Processing = Charge{Bps: d("1")}
	s.RailSurcharge = Charge{}
	b, err := Itemize(FeeInput{Amount: d("0.0000001"), Schedule: s, At: time.Now(), Precision: 7})
	if err != nil {
		t.Fatalf("Itemize: %v", err)
	}
	// 0.0000001 * 1/10000 = 1e-11, which must round up to one stroop-sized unit.
	if !b.Lines[1].Amount.Equal(d("0.0000001")) {
		t.Fatalf("processing = %s, want 0.0000001", b.Lines[1].Amount)
	}
}

func TestItemize_SurgeAppliesToRailOnlyAndRespectsCap(t *testing.T) {
	s := baseSchedule()
	s.Location, _ = time.LoadLocation("Africa/Lagos") // UTC+1, no DST
	s.RailSurcharge = Charge{Flat: d("1"), Max: dp("1.8")}
	s.SurgeWindows = []SurgeWindow{
		{Weekdays: []time.Weekday{time.Monday}, StartMinute: 8 * 60, EndMinute: 10 * 60, Multiplier: d("1.5")},
		{StartMinute: 9 * 60, EndMinute: 9*60 + 30, Multiplier: d("2")},
	}
	// Monday 08:15 UTC = 09:15 Lagos: both windows match, the higher (2) wins.
	at := time.Date(2026, 10, 19, 8, 15, 0, 0, time.UTC)
	b, err := Itemize(FeeInput{Amount: d("100"), Schedule: s, At: at, Precision: 7})
	if err != nil {
		t.Fatalf("Itemize: %v", err)
	}
	if !b.SurgeMultiplier.Equal(d("2")) {
		t.Fatalf("surge = %s, want 2", b.SurgeMultiplier)
	}
	if !b.Lines[2].Amount.Equal(d("1.8")) {
		t.Fatalf("rail = %s, want cap 1.8", b.Lines[2].Amount)
	}
	if !b.Lines[1].Amount.Equal(d("0.375")) { // 0.25 + 100*12.5/10000, no surge
		t.Fatalf("processing = %s, want 0.375", b.Lines[1].Amount)
	}
}

func TestSurgeMultiplier_Windows(t *testing.T) {
	night := SurgeWindow{Weekdays: []time.Weekday{time.Friday}, StartMinute: 22 * 60, EndMinute: 2 * 60, Multiplier: d("1.25")}
	tests := []struct {
		name string
		at   time.Time
		want string
	}{
		{"friday late part", time.Date(2026, 10, 23, 23, 0, 0, 0, time.UTC), "1.25"},
		{"saturday early part belongs to friday", time.Date(2026, 10, 24, 1, 59, 0, 0, time.UTC), "1.25"},
		{"end is exclusive", time.Date(2026, 10, 24, 2, 0, 0, 0, time.UTC), "1"},
		{"thursday late part does not match", time.Date(2026, 10, 22, 23, 0, 0, 0, time.UTC), "1"},
		{"friday early part opened thursday", time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC), "1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SurgeMultiplier([]SurgeWindow{night}, tc.at, time.UTC)
			if !got.Equal(d(tc.want)) {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestNetworkFeeStroops(t *testing.T) {
	if got := NetworkFeeStroops(3, 50, 0); got != 300 {
		t.Fatalf("below-minimum bid: got %d, want 300 (3 ops * 100 stroops)", got)
	}
	if got := NetworkFeeStroops(2, 1500, 12345); got != 15345 {
		t.Fatalf("got %d, want 15345", got)
	}
	if !StroopsToXLM(StroopsPerXLM).Equal(decimal.NewFromInt(1)) {
		t.Fatal("10,000,000 stroops must equal 1 XLM")
	}
}

func TestItemize_Validation(t *testing.T) {
	at := time.Now()
	bad := baseSchedule()
	bad.Processing.Max = dp("0.1")
	bad.Processing.Min = d("1")

	tests := []struct {
		name string
		in   FeeInput
		want error
	}{
		{"zero amount", FeeInput{Amount: decimal.Zero, Schedule: baseSchedule(), At: at}, ErrNonPositiveAmount},
		{"missing rate", FeeInput{Amount: d("1"), Schedule: baseSchedule(), NetworkFeeStroops: 100, At: at}, ErrInvalidRate},
		{"inverted bounds", FeeInput{Amount: d("1"), Schedule: bad, At: at}, ErrInvalidCharge},
		{"discount surge", FeeInput{Amount: d("1"), Schedule: func() CorridorSchedule {
			s := baseSchedule()
			s.SurgeWindows = []SurgeWindow{{StartMinute: 0, EndMinute: 60, Multiplier: d("0.5")}}
			return s
		}(), At: at}, ErrInvalidSurge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Itemize(tc.in); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

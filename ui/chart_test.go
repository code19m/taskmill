package ui

import (
	"math"
	"testing"
)

func TestDrawBreaksLinesAtGaps(t *testing.T) {
	t.Parallel()
	nan := math.NaN()
	for _, c := range []struct {
		values     []float64
		line, area string
	}{
		{[]float64{nan, nan}, "", ""},
		// A lone value is a dot; a gap starts a new run.
		{[]float64{4, nan, 0, 2}, "M0.5 4.0h0M2.5 96.0L3.5 50.0", "M0.5 96L0.5 4.0L0.5 96ZM2.5 96L2.5 96.0L3.5 50.0L3.5 96Z"},
	} {
		line, area, drawn := draw(c.values, 4)
		if line != c.line || area != c.area || drawn != (c.line != "") {
			t.Errorf("draw(%v) = %q, %q, %v; want %q, %q", c.values, line, area, drawn, c.line, c.area)
		}
	}
}

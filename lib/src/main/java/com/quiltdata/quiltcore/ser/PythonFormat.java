package com.quiltdata.quiltcore.ser;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * The PythonFormat class provides methods for formatting double values in Python-like format.
 */
public class PythonFormat {
    private static final DecimalFormat SCI_SMALL;
    private static final DecimalFormat SCI_LARGE;
    private static final DecimalFormat PLAIN;
    
    static {
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(Locale.US);

        symbols.setExponentSeparator("e");
        SCI_SMALL = new DecimalFormat("0.################E00", symbols);

        symbols.setExponentSeparator("e+");
        SCI_LARGE = new DecimalFormat("0.################E00", symbols);

        PLAIN     = new DecimalFormat("0.0###################", symbols);
    }

    /**
     * Formats a double value in Python-like format.
     * 
     * @param value The double value to be formatted.
     * @return The formatted string representation of the double value.
     */
    public static String formatDouble(double value) {
        double abs = Math.abs(value);
        if (abs > 0 && abs < 1.0e-4) {
            return SCI_SMALL.format(value);
        } else if (abs >= 1e16) {
            return SCI_LARGE.format(value);
        } else {
            return PLAIN.format(value);
        }
    }
}

package io.palyvos.provenance.l3stream.util;

import java.util.List;

public class FormatStimulusList {
    public static String formatStimulusList(List<Long> stimulusList) {
        String str = "";
        for (int i = 0; i < stimulusList.size()-1; i++) {
            str = str + stimulusList.get(i) + ",";
        }
        str = str + stimulusList.get(stimulusList.size()-1);
        return str;
    }
}

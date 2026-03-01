package org.lyon1.automaton;

import org.lyon1.path.ElementType;
import org.lyon1.path.GraphElement;
import org.lyon1.path.GraphPath;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternParser {

    public static GraphPath parsePattern(String input) {
        input = input.trim();
        List<GraphElement> elements = new ArrayList<>();

        // Group mapping:
        // 1: Node part including parentheses
        // 2: Node label
        // 3: Relationship part including arrows/dashes
        // 4: Incoming indicator (<)
        // 5: Relationship type
        // 6: Outgoing indicator (>)
        Pattern p = Pattern
                .compile("(\\(\\s*(?:\\w+\\s*)?:?\\s*([\\w|]+)\\s*\\))|((<)?\\s*-\\[\\s*:?([\\w|]+)\\s*\\]-\\s*(>)?)");
        Matcher m = p.matcher(input);

        while (m.find()) {
            if (m.group(1) != null) {
                // Node match
                elements.add(new GraphElement(0, m.group(2), ElementType.NODE, m.start()));
            } else if (m.group(3) != null) {
                // Relationship match
                boolean incoming = m.group(4) != null;
                elements.add(new GraphElement(0, m.group(5), ElementType.RELATIONSHIP, m.start(), incoming));
                //System.out.println("DEBUG: Parsed Rel [" + m.group(5) + "] incoming=" + incoming);
            }
        }

        elements.sort(Comparator.comparingInt(GraphElement::getPosition));
        return new GraphPath(elements);
    }
}

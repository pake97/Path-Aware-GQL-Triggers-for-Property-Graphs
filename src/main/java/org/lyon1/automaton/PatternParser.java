package org.lyon1.automaton;

import org.lyon1.path.ElementType;
import org.lyon1.path.GraphElement;
import org.lyon1.path.GraphPath;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternParser {

    public static GraphPath parsePattern(String input) {
        // Strip optional "Given " prefix and spaces
        input = input.trim();

        List<GraphElement> elements = new ArrayList<>();

        // Node: (a:Friend) or (:Friend) or (Friend) -> label=Friend
        Pattern nodePattern = Pattern.compile("\\((?:(\\w+))?:?(\\w+)\\)");
        Matcher nodeMatcher = nodePattern.matcher(input);
        while (nodeMatcher.find()) {
            String label = nodeMatcher.group(2);
            int pos = nodeMatcher.start();
            elements.add(new GraphElement(nodeMatcher.start(), label, ElementType.NODE, pos));
        }
        // Edge: [:FRIEND_OF] -> type=FRIEND_OF
        Pattern edgePattern = Pattern.compile("\\[:(\\w+)\\]");
        Matcher edgeMatcher = edgePattern.matcher(input);
        while (edgeMatcher.find()) {
            String type = edgeMatcher.group(1);
            int pos = edgeMatcher.start();
            elements.add(new GraphElement(edgeMatcher.start(), type, ElementType.RELATIONSHIP, pos));
        }

        // Sort by original position to keep correct order
        elements.sort(Comparator.comparingInt(GraphElement::getPosition));
        return new GraphPath(elements);

    }

}

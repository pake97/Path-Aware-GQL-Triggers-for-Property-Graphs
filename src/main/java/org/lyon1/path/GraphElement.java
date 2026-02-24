package org.lyon1.path;

public class GraphElement {
    final long id;
    final String label;
    final int position;
    final ElementType elementType;
    final boolean isIncoming;

    public GraphElement(long id, String label, ElementType type, int position) {
        this(id, label, type, position, false);
    }

    public GraphElement(long id, String label, ElementType type, int position, boolean isIncoming) {
        this.id = id;
        this.label = label;
        this.elementType = type;
        this.position = position;
        this.isIncoming = isIncoming;
    }

    @Override
    public String toString() {
        return (isRelationship() ? (isIncoming ? "<-" : "-") : "") + "n:" + label + " : " + id;
    }

    public long getId() {
        return id;
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getLabel() {
        return label;
    }

    public boolean isNode() {
        return elementType == ElementType.NODE;
    }

    public boolean isRelationship() {
        return elementType == ElementType.RELATIONSHIP;
    }

    public boolean isIncoming() {
        return isIncoming;
    }

    public int getPosition() {
        return position;
    }

}

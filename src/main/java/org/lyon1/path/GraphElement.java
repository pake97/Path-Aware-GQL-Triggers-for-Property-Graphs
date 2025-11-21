package org.lyon1.path;




public class GraphElement {
    final long id;
    final String label;
    final int position;
    final ElementType elementType;
    public GraphElement(long id, String label, ElementType type, int position) { this.id = id; this.label= label; this.elementType=type; this.position = position; }
    @Override public String toString() { return "n:" + label+" : " + id; }
    public long getId() { return id; }
    public ElementType getElementType() { return elementType; }
    public String getLabel() { return label; }
    public boolean isNode() { return elementType == ElementType.NODE; }
    public boolean isRelationship() { return elementType == ElementType.RELATIONSHIP; }
    public int getPosition() { return position;}

}


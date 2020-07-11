package io.mycat.hbt3;

public class SinglePartInfo implements PartInfo {
    private final Part part;

    public SinglePartInfo(Part part) {
        this.part = part;
    }
    @Override
    public int size() {
        return 1;
    }

    @Override
    public Part getPart(int index) {
        return part;
    }

    @Override
    public Part[] toPartArray() {
        return new Part[]{part};
    }

    @Override
    public String toString() {
        return "[ "+part.toString()+" ]";
    }
}
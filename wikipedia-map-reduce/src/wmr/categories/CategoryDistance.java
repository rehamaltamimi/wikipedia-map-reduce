package wmr.categories;

/**
 *
 * @author shilad
 */
public final class CategoryDistance implements Comparable<CategoryDistance> {
    private int catIndex;
    private double distance;
    private byte direction; // +1 or -1

    public CategoryDistance(int catIndex, double distance, byte direction) {
        this.catIndex = catIndex;
        this.distance = distance;
        this.direction = direction;
    }

    public final int getCatIndex() {
        return catIndex;
    }

    public final byte getDirection() {
        return direction;
    }

    public final double getDistance() {
        return distance;
    }

    public final int compareTo(CategoryDistance t) {
        if (distance < t.distance)
            return -1;
        else if (distance > t.distance)
            return 1;
        else
            return catIndex * direction - t.catIndex * t.direction;
    }
}

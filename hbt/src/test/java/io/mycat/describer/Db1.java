package io.mycat.describer;

public class Db1 {
    public final Travelrecord[] travelrecord = {
           new Travelrecord(1, 10),
            new    Travelrecord(2, 20),
    };
    public final Travelrecord[] travelrecord2 = {
            new Travelrecord(1, 10),
            new Travelrecord(2, 20),
    };

    public static class Travelrecord {
        public final int id;
        public final int user_id;

        public Travelrecord(int cust_id, int prod_id) {
            this.id = cust_id;
            this.user_id = prod_id;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || obj instanceof Travelrecord
                    && id == ((Travelrecord) obj).id
                    && user_id == ((Travelrecord) obj).user_id;
        }
    }
}
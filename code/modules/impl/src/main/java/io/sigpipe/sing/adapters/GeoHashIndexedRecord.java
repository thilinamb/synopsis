package io.sigpipe.sing.adapters;

public class GeoHashIndexedRecord {

    private byte[] payload;
    private String geohash;

    public GeoHashIndexedRecord(byte[] payload, String geohash) {
        this.payload = payload;
        this.geohash = geohash;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    public String getGeoHash() {
        return this.geohash;
    }

}

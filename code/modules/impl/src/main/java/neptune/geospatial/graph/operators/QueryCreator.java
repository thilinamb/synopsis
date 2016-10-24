package neptune.geospatial.graph.operators;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.SimplePair;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.query.Expression;
import io.sigpipe.sing.query.Operator;
import io.sigpipe.sing.query.Query;
import io.sigpipe.sing.query.RelationalQuery;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.util.ReducedTestConfiguration;

public class QueryCreator {

    private static Random random = new Random();

    public enum QueryType {
        Relational,
        Metadata,
    }

    public enum SpatialScope {
        Geo2500km,
        Geo630km,
        Geo78km,
    }

    private static Map<String, SimplePair<Double>> ranges = new HashMap<>();
    static {
        /* Set up ranges for all the features */
        for (String feature : ReducedTestConfiguration.FEATURE_NAMES) {
            Quantizer quant = ReducedTestConfiguration.quantizers.get(feature);
            double first = quant.first().getDouble();
            double last = quant.last().getDouble();
            SimplePair<Double> range = new SimplePair<>();
            range.a = first;
            range.b = last;
            ranges.put(feature, range);
        }
    }

    public static QueryWrapper create() {
        QueryType type = QueryType.values()[
            random.nextInt(QueryType.values().length)];

        SpatialScope scope = SpatialScope.values()[
            random.nextInt(SpatialScope.values().length)];

        return QueryCreator.create(type, scope);
    }

    public static QueryWrapper create(QueryType type, SpatialScope scope) {
        String geohash = "";
        switch (scope) {
            case Geo2500km:
                geohash = geo2500km[random.nextInt(geo2500km.length)];
                break;

            case Geo630km:
                geohash = geo630km[random.nextInt(geo630km.length)];
                break;

            case Geo78km:
                geohash = geo78km[random.nextInt(geo78km.length)];
                break;
        }

        /* Note: we're just using a RelationalQuery here to store the
         * expressions since Query is abstract (TODO: should Query really be
         * abstract, or should we provide another implementation for this use
         * case? */
        Query q = new RelationalQuery();

        q.addExpression(new Expression(
                    Operator.STR_PREFIX, new Feature("location", geohash)));

        int activeFeatures = ReducedTestConfiguration.FEATURE_NAMES.length;
        int numFeatures = 1 + random.nextInt(activeFeatures - 1);

        for (int i = 0; i < numFeatures; ++i) {
            String feature = ReducedTestConfiguration.FEATURE_NAMES[i];
            SimplePair<Double> range = ranges.get(feature);
            if (range == null) {
                continue;
            }

            if (range.a == 0.0 && range.b == 1.0) {
                /* Boolean feature */
                int value = random.nextInt(2);
                q.addExpression(
                        new Expression(
                            Operator.EQUAL,
                            new Feature(feature, (float) value)));
                continue;
            }

            double mag = range.b - range.a;
            double rangePerc = rangeSizes[random.nextInt(rangeSizes.length)];
            double size = mag * rangePerc;

            double startValue = randomRange(range.a, range.b);
            double endValue = startValue + size;

            q.addExpression(
                    new Expression(
                        Operator.RANGE_INC_EXC,
                        new Feature(feature, (float) startValue),
                        new Feature(feature, (float) endValue)));
        }

        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        SerializationOutputStream sOut = new SerializationOutputStream(
                new BufferedOutputStream(bOut));

        try {
            sOut.writeByte((byte) type.ordinal());
            sOut.writeSerializable(q);
            sOut.close();
            bOut.close();
        } catch (IOException e) {
            System.out.println("Error serializing query!");
            e.printStackTrace();
        }

        QueryWrapper qw = new QueryWrapper();
        qw.payload = bOut.toByteArray();
        qw.geohashes.add(geohash);

        return qw;
    }

    private static double randomRange(double low, double high) {
        double rand = random.nextDouble();
        rand *= high;
        rand -= low;
        return rand;
    }

    private static final double[] rangeSizes = { .2, .1, .05 };

    private static final String[] geo2500km = { "8", "9", "b", "c", "d", "f" };

    private static final String[] geo630km = {    "8g", "8u", "8v", "8x", "8y",
        "8z", "94", "95", "96", "97", "9d", "9e", "9g", "9h", "9j", "9k", "9m",
        "9n", "9p", "9q", "9r", "9s", "9t", "9u", "9v", "9w", "9x", "9y", "9z",
        "b8", "b9", "bb", "bc", "bf", "c0", "c1", "c2", "c3", "c4", "c6", "c8",
        "c9", "cb", "cc", "cd", "cf", "d4", "d5", "d6", "d7", "dd", "de", "dh",
        "dj", "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt", "dw", "dx", "dz",
        "f0", "f1", "f2", "f3", "f4", "f6", "f8", "f9", "fb", "fc", "fd", "ff"
    };

    private static final String[] geo78km = { "8gp", "8gr", "8gx", "8gy", "8gz",
        "8un", "8up", "8uq", "8ur", "8ut", "8uv", "8uw", "8ux", "8uy", "8uz",
        "8vg", "8vh", "8vj", "8vk", "8vm", "8vn", "8vp", "8vq", "8vr", "8vs",
        "8vt", "8vu", "8vv", "8vw", "8vx", "8vy", "8vz", "8xz", "8y5", "8y6",
        "8y7", "8yc", "8yd", "8ye", "8yf", "8yg", "8yh", "8yj", "8yk", "8ym",
        "8yn", "8yp", "8yq", "8yr", "8ys", "8yt", "8yu", "8yv", "8yw", "8yx",
        "8yy", "8yz", "8z1", "8z2", "8z3", "8z4", "8z5", "8z6", "8z7", "8z8",
        "8z9", "8zb", "8zc", "8zd", "8ze", "8zf", "8zg", "8zh", "8zj", "8zk",
        "8zm", "8zn", "8zp", "8zq", "8zr", "8zs", "8zt", "8zu", "8zv", "8zw",
        "8zx", "8zy", "8zz", "940", "941", "942", "943", "944", "946", "947",
        "948", "949", "94b", "94c", "94d", "94e", "94f", "94g", "94k", "94m",
        "94q", "94s", "94t", "94u", "94v", "94w", "94x", "94y", "94z", "950",
        "951", "952", "953", "954", "955", "956", "957", "958", "959", "95b",
        "95c", "95d", "95e", "95f", "95g", "95h", "95j", "95k", "95m", "95n",
        "95p", "95q", "95r", "95s", "95t", "95u", "95v", "95w", "95x", "95y",
        "95z", "968", "969", "96b", "96c", "96d", "96e", "96f", "96g", "96u",
        "96v", "96y", "96z", "970", "971", "972", "973", "974", "975", "976",
        "977", "978", "979", "97b", "97c", "97d", "97e", "97f", "97g", "97h",
        "97j", "97k", "97m", "97n", "97p", "97q", "97r", "97s", "97t", "97u",
        "97v", "97w", "97x", "97y", "97z", "9db", "9dc", "9df", "9e0", "9e1",
        "9e2", "9e3", "9e4", "9e5", "9e6", "9e7", "9e8", "9e9", "9eb", "9ec",
        "9ed", "9ee", "9ef", "9eg", "9eh", "9ej", "9ek", "9em", "9en", "9ep",
        "9eq", "9er", "9es", "9et", "9eu", "9ev", "9ew", "9ex", "9ey", "9ez",
        "9g0", "9g1", "9g2", "9g3", "9g4", "9g5", "9g6", "9g7", "9g8", "9g9",
        "9gb", "9gc", "9gd", "9ge", "9gf", "9gg", "9gh", "9gj", "9gk", "9gm",
        "9gn", "9gp", "9gq", "9gr", "9gs", "9gt", "9gu", "9gv", "9gw", "9gx",
        "9gy", "9gz", "9h0", "9h1", "9h2", "9h3", "9h4", "9h5", "9h6", "9h7",
        "9h8", "9h9", "9hb", "9hc", "9hd", "9he", "9hf", "9hg", "9hh", "9hj",
        "9hk", "9hm", "9hn", "9hp", "9hq", "9hr", "9hs", "9ht", "9hu", "9hv",
        "9hw", "9hx", "9hy", "9hz", "9j0", "9j1", "9j2", "9j3", "9j4", "9j5",
        "9j6", "9j7", "9j8", "9j9", "9jb", "9jc", "9jd", "9je", "9jf", "9jg",
        "9jh", "9jj", "9jk", "9jm", "9jn", "9jp", "9jq", "9jr", "9js", "9jt",
        "9ju", "9jv", "9jw", "9jx", "9jy", "9jz", "9k0", "9k1", "9k2", "9k3",
        "9k4", "9k5", "9k6", "9k7", "9k8", "9k9", "9kb", "9kc", "9kd", "9ke",
        "9kf", "9kg", "9kh", "9kj", "9kk", "9km", "9kn", "9kp", "9kq", "9kr",
        "9ks", "9kt", "9ku", "9kv", "9kw", "9kx", "9ky", "9kz", "9m0", "9m1",
        "9m2", "9m3", "9m4", "9m5", "9m6", "9m7", "9m8", "9m9", "9mb", "9mc",
        "9md", "9me", "9mf", "9mg", "9mh", "9mj", "9mk", "9mm", "9mn", "9mp",
        "9mq", "9mr", "9ms", "9mt", "9mu", "9mv", "9mw", "9mx", "9my", "9mz",
        "9n0", "9n1", "9n2", "9n3", "9n4", "9n5", "9n6", "9n7", "9n8", "9n9",
        "9nb", "9nc", "9nd", "9ne", "9nf", "9ng", "9nh", "9nj", "9nk", "9nm",
        "9nn", "9np", "9nq", "9nr", "9ns", "9nt", "9nu", "9nv", "9nw", "9nx",
        "9ny", "9nz", "9p0", "9p1", "9p2", "9p3", "9p4", "9p5", "9p6", "9p7",
        "9p8", "9p9", "9pb", "9pc", "9pd", "9pe", "9pf", "9pg", "9ph", "9pj",
        "9pk", "9pm", "9pn", "9pp", "9pq", "9pr", "9ps", "9pt", "9pu", "9pv",
        "9pw", "9px", "9py", "9pz", "9q0", "9q1", "9q2", "9q3", "9q4", "9q5",
        "9q6", "9q7", "9q8", "9q9", "9qb", "9qc", "9qd", "9qe", "9qf", "9qg",
        "9qh", "9qj", "9qk", "9qm", "9qn", "9qp", "9qq", "9qr", "9qs", "9qt",
        "9qu", "9qv", "9qw", "9qx", "9qy", "9qz", "9r0", "9r1", "9r2", "9r3",
        "9r4", "9r5", "9r6", "9r7", "9r8", "9r9", "9rb", "9rc", "9rd", "9re",
        "9rf", "9rg", "9rh", "9rj", "9rk", "9rm", "9rn", "9rp", "9rq", "9rr",
        "9rs", "9rt", "9ru", "9rv", "9rw", "9rx", "9ry", "9rz", "9s0", "9s1",
        "9s2", "9s3", "9s4", "9s5", "9s6", "9s7", "9s8", "9s9", "9sb", "9sc",
        "9sd", "9se", "9sf", "9sg", "9sh", "9sj", "9sk", "9sm", "9sn", "9sp",
        "9sq", "9sr", "9ss", "9st", "9su", "9sv", "9sw", "9sx", "9sy", "9sz",
        "9t0", "9t1", "9t2", "9t3", "9t4", "9t5", "9t6", "9t7", "9t8", "9t9",
        "9tb", "9tc", "9td", "9te", "9tf", "9tg", "9th", "9tj", "9tk", "9tm",
        "9tn", "9tp", "9tq", "9tr", "9ts", "9tt", "9tu", "9tv", "9tw", "9tx",
        "9ty", "9tz", "9u0", "9u1", "9u2", "9u3", "9u4", "9u5", "9u6", "9u7",
        "9u8", "9u9", "9ub", "9uc", "9ud", "9ue", "9uf", "9ug", "9uh", "9uj",
        "9uk", "9um", "9un", "9up", "9uq", "9ur", "9us", "9ut", "9uu", "9uv",
        "9uw", "9ux", "9uy", "9uz", "9v0", "9v1", "9v2", "9v3", "9v4", "9v5",
        "9v6", "9v7", "9v8", "9v9", "9vb", "9vc", "9vd", "9ve", "9vf", "9vg",
        "9vh", "9vj", "9vk", "9vm", "9vn", "9vp", "9vq", "9vr", "9vs", "9vt",
        "9vu", "9vv", "9vw", "9vx", "9vy", "9vz", "9w0", "9w1", "9w2", "9w3",
        "9w4", "9w5", "9w6", "9w7", "9w8", "9w9", "9wb", "9wc", "9wd", "9we",
        "9wf", "9wg", "9wh", "9wj", "9wk", "9wm", "9wn", "9wp", "9wq", "9wr",
        "9ws", "9wt", "9wu", "9wv", "9ww", "9wx", "9wy", "9wz", "9x0", "9x1",
        "9x2", "9x3", "9x4", "9x5", "9x6", "9x7", "9x8", "9x9", "9xb", "9xc",
        "9xd", "9xe", "9xf", "9xg", "9xh", "9xj", "9xk", "9xm", "9xn", "9xp",
        "9xq", "9xr", "9xs", "9xt", "9xu", "9xv", "9xw", "9xx", "9xy", "9xz",
        "9y0", "9y1", "9y2", "9y3", "9y4", "9y5", "9y6", "9y7", "9y8", "9y9",
        "9yb", "9yc", "9yd", "9ye", "9yf", "9yg", "9yh", "9yj", "9yk", "9ym",
        "9yn", "9yp", "9yq", "9yr", "9ys", "9yt", "9yu", "9yv", "9yw", "9yx",
        "9yy", "9yz", "9z0", "9z1", "9z2", "9z3", "9z4", "9z5", "9z6", "9z7",
        "9z8", "9z9", "9zb", "9zc", "9zd", "9ze", "9zf", "9zg", "9zh", "9zj",
        "9zk", "9zm", "9zn", "9zp", "9zq", "9zr", "9zs", "9zt", "9zu", "9zv",
        "9zw", "9zx", "9zy", "9zz", "b8p", "b8q", "b8r", "b8t", "b8v", "b8w",
        "b8x", "b8y", "b8z", "b97", "b9e", "b9g", "b9h", "b9j", "b9k", "b9m",
        "b9n", "b9p", "b9q", "b9r", "b9s", "b9t", "b9u", "b9v", "b9w", "b9x",
        "b9y", "b9z", "bb0", "bb1", "bb2", "bb3", "bb4", "bb5", "bb6", "bb7",
        "bb8", "bb9", "bbb", "bbc", "bbd", "bbe", "bbf", "bbg", "bbh", "bbj",
        "bbk", "bbm", "bbn", "bbp", "bbq", "bbr", "bbs", "bbt", "bbu", "bbv",
        "bbw", "bbx", "bby", "bbz", "bc0", "bc1", "bc2", "bc3", "bc4", "bc5",
        "bc6", "bc7", "bc8", "bc9", "bcb", "bcc", "bcd", "bce", "bcf", "bcg",
        "bch", "bcj", "bck", "bcm", "bcn", "bcp", "bcq", "bcr", "bcs", "bct",
        "bcu", "bcv", "bcw", "bcx", "bcy", "bcz", "bf0", "bf1", "bf4", "bf5",
        "bfh", "bfj", "bfm", "bfn", "bfp", "bfq", "bfr", "c00", "c01", "c02",
        "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c0b", "c0c", "c0d",
        "c0e", "c0f", "c0g", "c0h", "c0j", "c0k", "c0m", "c0n", "c0p", "c0q",
        "c0r", "c0s", "c0t", "c0u", "c0v", "c0w", "c0x", "c0y", "c0z", "c10",
        "c11", "c12", "c13", "c14", "c15", "c16", "c17", "c18", "c19", "c1b",
        "c1c", "c1d", "c1e", "c1f", "c1g", "c1h", "c1j", "c1k", "c1m", "c1n",
        "c1p", "c1q", "c1r", "c1s", "c1t", "c1u", "c1v", "c1w", "c1x", "c1y",
        "c1z", "c20", "c21", "c22", "c23", "c24", "c25", "c26", "c27", "c28",
        "c29", "c2b", "c2c", "c2d", "c2e", "c2f", "c2g", "c2h", "c2j", "c2k",
        "c2m", "c2n", "c2p", "c2q", "c2r", "c2s", "c2t", "c2u", "c2v", "c2w",
        "c2x", "c2y", "c2z", "c30", "c31", "c32", "c33", "c34", "c35", "c36",
        "c37", "c38", "c39", "c3b", "c3c", "c3d", "c3e", "c3f", "c3g", "c3h",
        "c3j", "c3k", "c3m", "c3n", "c3p", "c3q", "c3r", "c3s", "c3t", "c3u",
        "c3v", "c3w", "c3x", "c3y", "c3z", "c40", "c41", "c42", "c43", "c44",
        "c45", "c46", "c47", "c4e", "c4h", "c4j", "c4k", "c4m", "c4n", "c4p",
        "c4q", "c4r", "c4s", "c4t", "c4w", "c4x", "c60", "c61", "c62", "c63",
        "c64", "c65", "c66", "c67", "c68", "c69", "c6d", "c6e", "c6h", "c6j",
        "c6k", "c6m", "c6n", "c6p", "c6q", "c6r", "c6s", "c6t", "c6u", "c6v",
        "c6w", "c6x", "c6y", "c6z", "c80", "c81", "c82", "c83", "c84", "c85",
        "c86", "c87", "c88", "c89", "c8b", "c8c", "c8d", "c8e", "c8f", "c8g",
        "c8h", "c8j", "c8k", "c8m", "c8n", "c8p", "c8q", "c8r", "c8s", "c8t",
        "c8u", "c8v", "c8w", "c8x", "c8y", "c8z", "c90", "c91", "c92", "c93",
        "c94", "c95", "c96", "c97", "c98", "c99", "c9b", "c9c", "c9d", "c9e",
        "c9f", "c9g", "c9h", "c9j", "c9k", "c9m", "c9n", "c9p", "c9q", "c9r",
        "c9s", "c9t", "c9u", "c9v", "c9w", "c9x", "c9y", "c9z", "cb0", "cb1",
        "cb2", "cb3", "cb4", "cb5", "cb6", "cb7", "cb8", "cb9", "cbb", "cbc",
        "cbd", "cbe", "cbf", "cbg", "cbh", "cbj", "cbk", "cbm", "cbn", "cbp",
        "cbq", "cbr", "cbs", "cbt", "cbu", "cbv", "cbw", "cbx", "cby", "cbz",
        "cc0", "cc1", "cc2", "cc3", "cc4", "cc5", "cc6", "cc7", "cc8", "cc9",
        "ccb", "ccc", "ccd", "cce", "ccf", "ccg", "cch", "ccj", "cck", "ccm",
        "ccn", "ccp", "ccq", "ccr", "ccs", "cct", "ccu", "ccv", "ccw", "ccx",
        "ccy", "ccz", "cd0", "cd1", "cd2", "cd3", "cd4", "cd5", "cd6", "cd7",
        "cd8", "cd9", "cdb", "cdc", "cdd", "cde", "cdf", "cdg", "cdh", "cdj",
        "cdk", "cdm", "cdn", "cdp", "cdq", "cdr", "cds", "cdt", "cdu", "cdv",
        "cdw", "cdx", "cdy", "cdz", "cf0", "cf1", "cf2", "cf3", "cf4", "cf5",
        "cf6", "cf7", "cf8", "cf9", "cfb", "cfc", "cfd", "cfe", "cff", "cfg",
        "cfh", "cfj", "cfk", "cfm", "cfn", "cfp", "cfq", "cfr", "cfs", "cft",
        "cfu", "cfv", "cfw", "cfx", "cfy", "cfz", "d4y", "d4z", "d50", "d51",
        "d52", "d53", "d54", "d55", "d56", "d57", "d58", "d59", "d5b", "d5c",
        "d5d", "d5e", "d5f", "d5g", "d5h", "d5j", "d5k", "d5m", "d5n", "d5p",
        "d5q", "d5r", "d5s", "d5t", "d5u", "d5v", "d5w", "d5x", "d5y", "d5z",
        "d6b", "d6c", "d6f", "d6g", "d6t", "d6u", "d6v", "d6w", "d6x", "d6y",
        "d6z", "d70", "d71", "d72", "d73", "d74", "d75", "d76", "d77", "d78",
        "d79", "d7b", "d7c", "d7d", "d7e", "d7f", "d7g", "d7h", "d7j", "d7k",
        "d7m", "d7n", "d7p", "d7q", "d7r", "d7s", "d7t", "d7u", "d7v", "d7w",
        "d7x", "d7y", "d7z", "dd8", "dd9", "ddb", "ddc", "ddf", "de0", "de1",
        "de2", "de3", "de4", "de6", "de8", "de9", "deb", "dec", "ded", "def",
        "deg", "dh0", "dh1", "dh2", "dh3", "dh4", "dh5", "dh6", "dh7", "dh8",
        "dh9", "dhb", "dhc", "dhd", "dhe", "dhf", "dhg", "dhh", "dhj", "dhk",
        "dhm", "dhn", "dhp", "dhq", "dhr", "dhs", "dht", "dhu", "dhv", "dhw",
        "dhx", "dhy", "dhz", "dj0", "dj1", "dj2", "dj3", "dj4", "dj5", "dj6",
        "dj7", "dj8", "dj9", "djb", "djc", "djd", "dje", "djf", "djg", "djh",
        "djj", "djk", "djm", "djn", "djp", "djq", "djr", "djs", "djt", "dju",
        "djv", "djw", "djx", "djy", "djz", "dk0", "dk1", "dk2", "dk3", "dk4",
        "dk5", "dk6", "dk7", "dk8", "dk9", "dkb", "dkc", "dkd", "dke", "dkf",
        "dkg", "dkh", "dkj", "dkk", "dkm", "dkn", "dkp", "dkq", "dkr", "dks",
        "dkt", "dku", "dkv", "dkw", "dkx", "dky", "dkz", "dm0", "dm1", "dm2",
        "dm3", "dm4", "dm5", "dm6", "dm7", "dm8", "dm9", "dmb", "dmc", "dmd",
        "dme", "dmf", "dmg", "dmh", "dmj", "dmk", "dmm", "dmn", "dmp", "dmq",
        "dmr", "dms", "dmt", "dmu", "dmv", "dmw", "dmx", "dmy", "dmz", "dn0",
        "dn1", "dn2", "dn3", "dn4", "dn5", "dn6", "dn7", "dn8", "dn9", "dnb",
        "dnc", "dnd", "dne", "dnf", "dng", "dnh", "dnj", "dnk", "dnm", "dnn",
        "dnp", "dnq", "dnr", "dns", "dnt", "dnu", "dnv", "dnw", "dnx", "dny",
        "dnz", "dp0", "dp1", "dp2", "dp3", "dp4", "dp5", "dp6", "dp7", "dp8",
        "dp9", "dpb", "dpc", "dpd", "dpe", "dpf", "dpg", "dph", "dpj", "dpk",
        "dpm", "dpn", "dpp", "dpq", "dpr", "dps", "dpt", "dpu", "dpv", "dpw",
        "dpx", "dpy", "dpz", "dq0", "dq1", "dq2", "dq3", "dq4", "dq5", "dq6",
        "dq7", "dq8", "dq9", "dqb", "dqc", "dqd", "dqe", "dqf", "dqg", "dqh",
        "dqj", "dqk", "dqm", "dqn", "dqp", "dqq", "dqr", "dqs", "dqt", "dqu",
        "dqv", "dqw", "dqx", "dqy", "dqz", "dr0", "dr1", "dr2", "dr3", "dr4",
        "dr5", "dr6", "dr7", "dr8", "dr9", "drb", "drc", "drd", "dre", "drf",
        "drg", "drh", "drj", "drk", "drm", "drn", "drp", "drq", "drr", "drs",
        "drt", "dru", "drv", "drw", "drx", "dry", "drz", "ds0", "ds1", "ds2",
        "ds3", "ds4", "ds5", "ds6", "ds7", "ds8", "ds9", "dsb", "dsc", "dsd",
        "dse", "dsf", "dsg", "dsu", "dt0", "dt1", "dt2", "dt3", "dt4", "dt5",
        "dt6", "dt7", "dt8", "dt9", "dtb", "dtc", "dtd", "dte", "dtf", "dtg",
        "dth", "dtk", "dts", "dtt", "dtu", "dtv", "dw0", "dw1", "dw2", "dw3",
        "dw4", "dw5", "dw6", "dw7", "dw8", "dw9", "dwb", "dwc", "dwd", "dwe",
        "dwf", "dwg", "dwh", "dwj", "dwk", "dwm", "dwq", "dws", "dwt", "dwu",
        "dwv", "dww", "dwy", "dx0", "dx1", "dx2", "dx3", "dx4", "dx5", "dx6",
        "dx7", "dx8", "dx9", "dxb", "dxc", "dxd", "dxe", "dxf", "dxg", "dxh",
        "dxj", "dxk", "dxm", "dxn", "dxp", "dxq", "dxr", "dxs", "dxt", "dxu",
        "dxv", "dxw", "dxx", "dxy", "dxz", "dzb", "f00", "f01", "f02", "f03",
        "f04", "f05", "f06", "f07", "f08", "f09", "f0b", "f0c", "f0d", "f0e",
        "f0f", "f0g", "f0h", "f0j", "f0k", "f0m", "f0n", "f0p", "f0q", "f0r",
        "f0s", "f0t", "f0u", "f0v", "f0w", "f0x", "f0y", "f0z", "f10", "f11",
        "f12", "f13", "f14", "f15", "f16", "f17", "f18", "f19", "f1b", "f1c",
        "f1d", "f1e", "f1f", "f1g", "f1h", "f1j", "f1k", "f1m", "f1n", "f1p",
        "f1q", "f1r", "f1s", "f1t", "f1u", "f1v", "f1w", "f1x", "f1y", "f1z",
        "f20", "f21", "f22", "f23", "f24", "f25", "f26", "f27", "f28", "f29",
        "f2b", "f2c", "f2d", "f2e", "f2f", "f2g", "f2h", "f2j", "f2k", "f2m",
        "f2n", "f2p", "f2q", "f2r", "f2s", "f2t", "f2u", "f2v", "f2w", "f2x",
        "f2y", "f2z", "f30", "f31", "f32", "f33", "f34", "f35", "f36", "f37",
        "f38", "f39", "f3b", "f3c", "f3d", "f3e", "f3f", "f3g", "f3h", "f3j",
        "f3k", "f3m", "f3n", "f3p", "f3q", "f3r", "f3s", "f3t", "f3u", "f3v",
        "f3w", "f3x", "f3y", "f3z", "f40", "f41", "f42", "f43", "f44", "f45",
        "f46", "f47", "f48", "f49", "f4b", "f4c", "f4d", "f4e", "f4f", "f4g",
        "f4h", "f4j", "f4k", "f4m", "f4n", "f4p", "f4q", "f4r", "f4s", "f4t",
        "f4u", "f4v", "f4w", "f4x", "f4y", "f4z", "f60", "f61", "f62", "f63",
        "f64", "f65", "f66", "f67", "f68", "f69", "f6b", "f6c", "f6d", "f6e",
        "f6f", "f6g", "f6h", "f6j", "f6k", "f6m", "f6n", "f6p", "f6q", "f6r",
        "f6s", "f6t", "f6w", "f6x", "f80", "f81", "f82", "f83", "f84", "f85",
        "f86", "f87", "f88", "f89", "f8b", "f8c", "f8d", "f8e", "f8f", "f8g",
        "f8h", "f8j", "f8k", "f8m", "f8n", "f8p", "f8q", "f8r", "f8s", "f8t",
        "f8u", "f8v", "f8w", "f8x", "f8y", "f8z", "f90", "f91", "f92", "f93",
        "f94", "f95", "f96", "f97", "f98", "f99", "f9b", "f9c", "f9d", "f9e",
        "f9f", "f9g", "f9h", "f9j", "f9k", "f9m", "f9n", "f9p", "f9q", "f9r",
        "f9s", "f9t", "f9u", "f9v", "f9w", "f9x", "f9y", "f9z", "fb0", "fb2",
        "fb3", "fb8", "fb9", "fbb", "fbc", "fbf", "fc0", "fc1", "fc2", "fc3",
        "fc4", "fc6", "fc7", "fc8", "fc9", "fcb", "fcc", "fcd", "fce", "fcf",
        "fcg", "fcu", "fd0", "fd1", "fd2", "fd3", "fd4", "fd5", "fd6", "fd7",
        "fd8", "fd9", "fdd", "fde", "fdh", "fdj", "fdk", "fdm", "fdn", "fdp",
        "fdq", "fdr", "fds", "fdt", "ff0", "ff1", "ff2", "ff3", "ff4", "ff5",
        "ff6", "ff7", "ffh", };
}

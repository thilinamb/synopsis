package neptune.geospatial.benchmarks.airquality;

import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class Configuration {
    public static String[] FEATURE_NAMES = new String[]{
            "Ozone",
            "Carbon monoxide",
            "Nitrogen dioxide (NO2)",
            "Sulfur dioxide"
    };

    public static final Map<String, Quantizer> quantizers = new HashMap<>();

    static {
        quantizers.put("Ozone", new Quantizer(
                new Feature(-0.005),
                new Feature(0.005028571428571428),
                new Feature(0.015057142857142856),
                new Feature(0.025085714285714284),
                new Feature(0.035114285714285716),
                new Feature(0.045142857142857144),
                new Feature(0.05517142857142857),
                new Feature(0.0652),
                new Feature(0.07522857142857142),
                new Feature(0.08525714285714285),
                new Feature(0.09528571428571428),
                new Feature(0.10531428571428571),
                new Feature(0.11534285714285714),
                new Feature(0.12537142857142858),
                new Feature(0.1354),
                new Feature(0.1454285714285714),
                new Feature(0.15545714285714285),
                new Feature(0.1654857142857143),
                new Feature(0.1755142857142857),
                new Feature(0.18554285714285712),
                new Feature(0.19557142857142856),
                new Feature(0.2056),
                new Feature(0.21562857142857142),
                new Feature(0.22565714285714283),
                new Feature(0.23568571428571428),
                new Feature(0.24571428571428572),
                new Feature(0.25574285714285716),
                new Feature(0.26577142857142855),
                new Feature(0.2758),
                new Feature(0.28582857142857143),
                new Feature(0.2958571428571428),
                new Feature(0.30588571428571426),
                new Feature(0.3159142857142857),
                new Feature(0.32594285714285715),
                new Feature(0.3359714285714286),
                new Feature(0.346)));

        quantizers.put("Carbon monoxide", new Quantizer(
                new Feature(-0.7),
                new Feature(0.056600000000002225),
                new Feature(0.12095000000000225),
                new Feature(0.15215000000000234),
                new Feature(0.18042500000000244),
                new Feature(0.21747500000000258),
                new Feature(0.2759750000000028),
                new Feature(0.31692500000000295),
                new Feature(0.4066250000000033),
                new Feature(0.613325000000004),
                new Feature(3.202924999999945)));

        quantizers.put("Nitrogen dioxide (NO2)", new Quantizer(
                new Feature(-5.599999999999995),
                new Feature(0.025142857142885847),
                new Feature(0.5377142857143175),
                new Feature(0.958285714285748),
                new Feature(1.4051428571428939),
                new Feature(1.9834285714286122),
                new Feature(2.666857142857188),
                new Feature(3.402857142857193),
                new Feature(4.349142857142911),
                new Feature(5.716000000000052),
                new Feature(7.50342857142862),
                new Feature(10.079428571428652),
                new Feature(14.49542857142872),
                new Feature(22.74914285714313),
                new Feature(68.01314285714139)));

        quantizers.put("Sulfur dioxide", new Quantizer(
                new Feature(-3.5000000000000013),
                new Feature(-0.1243999999999853),
                new Feature(0.010450000000015423),
                new Feature(0.12355000000001604),
                new Feature(0.24970000000001671),
                new Feature(0.4585000000000177),
                new Feature(0.7717000000000193),
                new Feature(1.0675000000000208),
                new Feature(1.8679000000000245),
                new Feature(3.3991000000000318),
                new Feature(31.30870000000051)));
    }
}

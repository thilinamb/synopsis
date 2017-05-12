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
                new Feature(-0.005f),
                new Feature(0.005028571428571428f),
                new Feature(0.015057142857142856f),
                new Feature(0.025085714285714284f),
                new Feature(0.035114285714285716f),
                new Feature(0.045142857142857144f),
                new Feature(0.05517142857142857f),
                new Feature(0.0652f),
                new Feature(0.07522857142857142f),
                new Feature(0.08525714285714285f),
                new Feature(0.09528571428571428f),
                new Feature(0.10531428571428571f),
                new Feature(0.11534285714285714f),
                new Feature(0.12537142857142858f),
                new Feature(0.1354f),
                new Feature(0.1454285714285714f),
                new Feature(0.15545714285714285f),
                new Feature(0.1654857142857143f),
                new Feature(0.1755142857142857f),
                new Feature(0.18554285714285712f),
                new Feature(0.19557142857142856f),
                new Feature(0.2056f),
                new Feature(0.21562857142857142f),
                new Feature(0.22565714285714283f),
                new Feature(0.23568571428571428f),
                new Feature(0.24571428571428572f),
                new Feature(0.25574285714285716f),
                new Feature(0.26577142857142855f),
                new Feature(0.2758f),
                new Feature(0.28582857142857143f),
                new Feature(0.2958571428571428f),
                new Feature(0.30588571428571426f),
                new Feature(0.3159142857142857f),
                new Feature(0.32594285714285715f),
                new Feature(0.3359714285714286f),
                new Feature(0.346f)));

        quantizers.put("Carbon monoxide", new Quantizer(
                new Feature(-0.7f),
                new Feature(0.056600000000002225f),
                new Feature(0.12095000000000225f),
                new Feature(0.15215000000000234f),
                new Feature(0.18042500000000244f),
                new Feature(0.21747500000000258f),
                new Feature(0.2759750000000028f),
                new Feature(0.31692500000000295f),
                new Feature(0.4066250000000033f),
                new Feature(0.613325000000004f),
                new Feature(3.202924999999945f)));

        quantizers.put("Nitrogen dioxide (NO2)", new Quantizer(
                new Feature(-5.599999999999995f),
                new Feature(0.025142857142885847f),
                new Feature(0.5377142857143175f),
                new Feature(0.958285714285748f),
                new Feature(1.4051428571428939f),
                new Feature(1.9834285714286122f),
                new Feature(2.666857142857188f),
                new Feature(3.402857142857193f),
                new Feature(4.349142857142911f),
                new Feature(5.716000000000052f),
                new Feature(7.50342857142862f),
                new Feature(10.079428571428652f),
                new Feature(14.49542857142872f),
                new Feature(22.74914285714313f),
                new Feature(68.01314285714139f)));

        quantizers.put("Sulfur dioxide", new Quantizer(
                new Feature(-3.5000000000000013f),
                new Feature(-0.1243999999999853f),
                new Feature(0.010450000000015423f),
                new Feature(0.12355000000001604f),
                new Feature(0.24970000000001671f),
                new Feature(0.4585000000000177f),
                new Feature(0.7717000000000193f),
                new Feature(1.0675000000000208f),
                new Feature(1.8679000000000245f),
                new Feature(3.3991000000000318f),
                new Feature(31.30870000000051f)));
    }
}

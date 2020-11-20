package hagan.b.nifi.processors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
//import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import com.fasterxml.jackson.databind.ObjectMapper;


@SideEffectFree
@Tags({"generate", "data"})
@CapabilityDescription("Generate random numbers within a range")

public class RandomDataGenerator extends AbstractProcessor {
    DecimalFormat df = new DecimalFormat();
    String randomNumber = "";
    String data = "";
    LinkedHashMap<String, String> contentList = new LinkedHashMap<>();
    Timestamp timestamp;
    ArrayList<String> nameList = new ArrayList<String>();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final String MIN_TEXT = "min";
    public static final String MAX_TEXT = "max";
    public static final String CREATE_ATTRIBUTES_TRUE = "true";
    public static final String CREATE_ATTRIBUTES_FALSE = "false";
    public static final String EVENT_TIME_TEXT = "timestamp";
    public static final String METRIC_TEXT_DEFAULT = "metric";
    public static final String METRIC_TYPE_INTEGER = "Integer";
    public static final String METRIC_TYPE_DOUBLE = "Double";
    public static final String OUTPUT_FORMAT_JSON = "json";
    public static final String OUTPUT_FORMAT_CSV = "csv";
    public static final String RANDOM_NUMBER_DEFAULT = "0";
    public static final String APPLICATION_JSON = "application/json";
    private static final String OUTPUT_MIME_TYPE = "text/csv";
    private static final String OUTPUT_SEPARATOR = ",";
    public static final String IDENTIFIER_DEFAULT = "id";
    public static final String CREATE_NAMES_TRUE = "true";
    public static final String CREATE_NAMES_FALSE = "false";
    public static final String LNAME_TEXT = "lname";
    public static final String FNAME_TEXT = "fname";
    public static final String NAME_RANGE_TRUE = "true";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final PropertyDescriptor MIN = new PropertyDescriptor.Builder()
            .name("Min")
            .description("Minimum value inclusive")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX = new PropertyDescriptor.Builder()
            .name("Max")
            .description("Maximum value inclusive")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor METRIC_TYPE = new PropertyDescriptor.Builder()
            .name("Metric Type")
            .description("Generate Integers or Doubles")
            .required(true)
            .defaultValue(METRIC_TYPE_INTEGER)
            .allowableValues(METRIC_TYPE_INTEGER, METRIC_TYPE_DOUBLE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DOUBLE_PRECISION = new PropertyDescriptor.Builder()
            .name("Precision")
            .description("Set the metric precision")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor METRIC_NAME = new PropertyDescriptor.Builder()
            .name("Metric Name")
            .description("Set the metric name")
            .required(false)
            .defaultValue(METRIC_TEXT_DEFAULT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("Output Format")
            .description("Set the output format")
            .required(false)
            .defaultValue(OUTPUT_FORMAT_CSV)
            .allowableValues(OUTPUT_FORMAT_JSON, OUTPUT_FORMAT_CSV)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Identifier")
            .description("Provide an identifier name")
            .required(false)
            .defaultValue(IDENTIFIER_DEFAULT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CREATE_NAMES = new PropertyDescriptor.Builder()
            .name("Create Names")
            .description("Creates first and last names")
            .required(false)
            .defaultValue(CREATE_NAMES_TRUE)
            .allowableValues(CREATE_NAMES_TRUE, CREATE_NAMES_FALSE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor NAME_SET = new PropertyDescriptor.Builder()
            .name("Name Set")
            .description("Number of random names")
            .required(false)
            .defaultValue(CREATE_NAMES_TRUE)
            .allowableValues(CREATE_NAMES_TRUE, CREATE_NAMES_FALSE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CREATE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Create Attributes")
            .description("True writes Attributes")
            .required(false)
            .defaultValue(CREATE_ATTRIBUTES_TRUE)
            .allowableValues(CREATE_ATTRIBUTES_TRUE, CREATE_ATTRIBUTES_FALSE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success Relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MIN);
        properties.add(MAX);
        properties.add(METRIC_TYPE);
        properties.add(DOUBLE_PRECISION);
        properties.add(OUTPUT_FORMAT);
        properties.add(METRIC_NAME);
        properties.add(IDENTIFIER);
        properties.add(CREATE_ATTRIBUTES);
        properties.add(CREATE_NAMES);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        String csvFile = "/Users/bhagan/Documents/fnamelname.csv";
        String line = "";
        String cvsSplitBy = ",";
        int mapKey = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                String[] name = line.split(cvsSplitBy);
                String flname = name[0] + "," + name[1];
                nameList.add(flname);
                mapKey++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private LinkedHashMap<String, String> generateData(final ProcessContext context) {
        Random r = new Random();
        int min = new Integer(context.getProperty(MIN).getValue());
        int max = new Integer(context.getProperty(MAX).getValue());
        String metric_text = context.getProperty(METRIC_NAME).getValue();
        String id = Integer.toString(r.ints(1, 101).limit(1).findFirst().getAsInt());

        if(context.getProperty(METRIC_TYPE).getValue().equalsIgnoreCase(METRIC_TYPE_DOUBLE)) {
            df.setMaximumFractionDigits(context.getProperty(DOUBLE_PRECISION).asInteger());
            randomNumber = df.format(r.doubles(min, (max + 1)).limit(1).findFirst().getAsDouble());
        }
        else randomNumber = Integer.toString(r.ints(min, (max + 1)).limit(1).findFirst().getAsInt());

        timestamp = new Timestamp(System.currentTimeMillis());

        int nameIndex = r.ints(0, 101).limit(1).findFirst().getAsInt();
        String name = nameList.get(nameIndex);


        LinkedHashMap<String, String> cList = new LinkedHashMap<>();
        cList.put(MIN_TEXT, Integer.toString(min));
        cList.put(MAX_TEXT, Integer.toString(max));
        cList.put(metric_text, randomNumber);
        cList.put(context.getProperty(IDENTIFIER).getValue(), id);
        cList.put(EVENT_TIME_TEXT, timestamp.toString());
        cList.put("lname", name.substring(0, name.indexOf(",")));
        cList.put("fname", name.substring(name.indexOf(",") + 1));


        return cList;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        contentList = generateData(context);

        FlowFile flowfile = session.create();
        if(context.getProperty(CREATE_ATTRIBUTES).getValue().equalsIgnoreCase("true")) {
            flowfile = session.putAttribute(flowfile, "min", context.getProperty(MIN).getValue());
            flowfile = session.putAttribute(flowfile, "max", context.getProperty(MAX).getValue());
            flowfile = session.putAttribute(flowfile, "randomNumber", randomNumber);
            flowfile = session.putAttribute(flowfile, "id", context.getProperty(IDENTIFIER).getValue());
            flowfile = session.putAttribute(flowfile,  "metric_name", context.getProperty(METRIC_NAME).getValue());
            flowfile = session.putAttribute(flowfile, "timestamp", timestamp.toString());
            flowfile = session.putAttribute(flowfile, LNAME_TEXT, contentList.get("lname"));
            flowfile = session.putAttribute(flowfile, FNAME_TEXT, contentList.get("fname"));
        }


        if(context.getProperty(OUTPUT_FORMAT).getValue().equalsIgnoreCase("json")) {
            flowfile = session.write(flowfile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(objectMapper.writeValueAsBytes(contentList));
                }
            });
            flowfile = session.putAttribute(flowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
        }
        else
        {
            final StringBuilder sbNames = new StringBuilder();
            final StringBuilder sbValues = new StringBuilder();
            final int fieldListSize = contentList.values().size() -1;

            int index = 0;

            for (final Map.Entry<String, String> field : contentList.entrySet()) {
                sbNames.append(StringEscapeUtils.escapeCsv(field.getKey()));
                sbNames.append(index++ < fieldListSize ? OUTPUT_SEPARATOR : "");
            }

            index = 0;
            for (final Map.Entry<String, String> field : contentList.entrySet()) {
                sbValues.append(StringEscapeUtils.escapeCsv(field.getValue()));
                sbValues.append(index++ < fieldListSize ? OUTPUT_SEPARATOR : "");
            }

            flowfile = session.write(flowfile, out -> {
                sbNames.append(System.getProperty("line.separator"));
                out.write(sbNames.toString().getBytes());
                out.write(sbValues.toString().getBytes());
            });
            flowfile = session.putAttribute(flowfile, CoreAttributes.MIME_TYPE.key(), OUTPUT_MIME_TYPE);
        }

        session.transfer(flowfile, SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
}

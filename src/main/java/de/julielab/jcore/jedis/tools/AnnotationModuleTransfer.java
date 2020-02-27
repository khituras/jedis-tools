package de.julielab.jcore.jedis.tools;

import de.julielab.costosys.cli.TableNotFoundException;
import de.julielab.costosys.configuration.FieldConfig;
import de.julielab.costosys.dbconnection.DBCIterator;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.costosys.dbconnection.util.CoStoSysException;
import de.julielab.costosys.dbconnection.util.CoStoSysJedisTools;
import de.julielab.costosys.dbconnection.util.TableSchemaMismatchException;
import de.julielab.xml.*;
import de.julielab.xml.binary.BinaryDecodingResult;
import de.julielab.xml.binary.BinaryJeDISNodeDecoder;
import de.julielab.xml.binary.BinaryXmiBuilder;
import de.julielab.xml.util.XMIBuilderException;
import de.julielab.xml.util.XMISplitterException;
import org.apache.commons.io.FileUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Reads the annotation modules from one JeDIS database and adds them to another JeDIS database.
 */
public class AnnotationModuleTransfer {
    private final static Logger log = LoggerFactory.getLogger(AnnotationModuleTransfer.class);

    public static void main(String args[]) throws Exception {
        File srcCostosysFile = new File("costosys-19.xml");
        File targetCostosysFile = new File("costosys-20.xml");
        String annotationModuleName = "flairgenes:de.julielab.jcore.types.EmbeddingVector";
        String targetModuleName = "flairgenes:de.julielab.jcore.types.EmbeddingVector";
        String srcTextHashColumnName = "document_text_sha256";
        String targetTextHashColumnName = "document_text_sha256";
        boolean targetZip = true;
        boolean targetBinary = true;
        File shaMismatches = new File("shaMismatches.txt");
        AnnotationModuleTransfer.transferAnnotationModules(srcCostosysFile, targetCostosysFile, annotationModuleName, targetModuleName, srcTextHashColumnName, targetTextHashColumnName, targetZip, targetBinary, shaMismatches);
    }

    /**
     * @param srcCostosysConfig
     * @param targetCostosysConfig
     * @param srcAnnotationModuleName    Potentially qualified name of the annotation module in the source database as UIMA type class. The qualifier should be separated by a colon ':' from the type name.
     * @param targetAnnotationModuleName Potentially qualified name of the annotation module as it should be written to the target database as UIMA type class. The qualifier should be separated by a colon ':' from the type name.
     * @param targetGzip
     * @param targetBinary
     * @param shaMismatches
     * @throws FileNotFoundException
     */
    public static void transferAnnotationModules(File srcCostosysConfig, File targetCostosysConfig, String srcAnnotationModuleName, String targetAnnotationModuleName, String srcTextHashColumnName, String targetTextHashColumnName, boolean targetGzip, boolean targetBinary, File shaMismatches) throws IOException, CoStoSysException, TableNotFoundException, UIMAException, XMIBuilderException {
        if (!srcCostosysConfig.exists())
            throw new FileNotFoundException("The source CoStoSys configuration file " + srcCostosysConfig.getAbsolutePath() + " does not exist.");
        if (!targetCostosysConfig.exists())
            throw new FileNotFoundException("The target CoStoSys configuration file " + srcCostosysConfig.getAbsolutePath() + " does not exist.");

        String table = "_data_xmi.documents";

        DataBaseConnector srcDbc = new DataBaseConnector(srcCostosysConfig.getAbsolutePath());
        addSrcXmiSchema(table, srcAnnotationModuleName, srcTextHashColumnName, srcDbc);
        CoStoSysJedisTools.JedisDataFormat srcDf = CoStoSysJedisTools.determineDataFormat(srcDbc, table, srcAnnotationModuleName.toLowerCase().replace('.', '_').replace(':', '$'));
        BinaryJeDISNodeDecoder binaryDecoder = null;
        BinaryXmiBuilder binaryXmiBuilder = null;
        Map<Integer, String> reversedBinaryMapping = null;
        Map<String, Boolean> mappedFeatures = null;
        Map<String, String> nsMap = null;
        TypeSystem ts = null;
        if (srcDf.isBinary()) {
            nsMap = CoStoSysJedisTools.getNamespaceMap(srcDbc, "public");
            reversedBinaryMapping = CoStoSysJedisTools.getReverseBinaryMappingFromDb(srcDbc, "public");
            mappedFeatures = CoStoSysJedisTools.getFeaturesToMapBinaryFromDb(srcDbc, "public");
            String unqualifiedTypeName = srcAnnotationModuleName;
            if (unqualifiedTypeName.contains(":"))
                unqualifiedTypeName = unqualifiedTypeName.split(":")[1];
            binaryDecoder = new BinaryJeDISNodeDecoder(Collections.singleton(unqualifiedTypeName), false);
            binaryXmiBuilder = new BinaryXmiBuilder(nsMap);
            ts = JCasFactory.createJCas("de.julielab.jcore.types.jcore-all-types").getTypeSystem();
        }

        DataBaseConnector targetDbc = new DataBaseConnector(targetCostosysConfig.getAbsolutePath());
        addTargetColumnsAndXmiSchema(targetAnnotationModuleName, targetTextHashColumnName, targetGzip, targetBinary, table, targetDbc);
        String targetMaxXmiHashSofaMapConfigName = addTargetMaxXmiHashAndSofaMapSchema(table, targetTextHashColumnName, targetGzip, targetBinary, targetDbc);

        String where = "pmid in ('29301516', '10926647', '29300724', '29301517', '29300758', '29300726', '29300716', '10926674', '29300728', '29297116')";
        DBCIterator<byte[][]> moduleData = srcDbc.queryDataTable(table, where);
        Map<String, ModuleTransferInfo> transferBatch = new HashMap<>();
        while (moduleData.hasNext()) {
            // 1. module aus source mit max xmi id, sha hash und sofa map erhalten
            // receiving the following columns:  "docid", "max_xmi_id", "sofa_mapping", srcTextHashColumnName, <annotation module>
            byte[][] srcData = moduleData.next();
            String docId = new String(srcData[0], UTF_8);
            Map<Integer, String> idToSofaName = parseSofaIdMap(srcData[1], docId);
            byte[] srcSha = srcData[2];
            byte[] annotationModuleData = srcData[3];
            ModuleTransferInfo transferInfo = new ModuleTransferInfo(docId, idToSofaName, srcSha, annotationModuleData);
            transferBatch.put(docId, transferInfo);
            if (transferBatch.size() == 1000 || !moduleData.hasNext()) {
                List<AdaptedAnnotationModuleData> transferData = new ArrayList<>(1000);
                // retrieve max xmi ID and sofa map from the target database documents
                DBCIterator<byte[][]> targetDocInfo = targetDbc.retrieveColumnsByTableSchema(transferBatch.keySet().stream().map(id -> new Object[]{id}).collect(Collectors.toList()), table, targetMaxXmiHashSofaMapConfigName);
                // only do this conversion if the sha matches
                while (targetDocInfo.hasNext()) {
                    byte[][] next = targetDocInfo.next();
                    String targetDocId = new String(next[0], UTF_8);
                    int maxXmiId = Integer.parseInt(new String(next[1]));
                    Map<Integer, String> targetIdToSofaName = parseSofaIdMap(next[2], targetDocId);
                    byte[] targetSha = next[3];
                    ModuleTransferInfo srcInfo = transferBatch.get(targetDocId);
                    if (Arrays.equals(targetSha, srcInfo.getSrcSha())) {
                        byte[] originalAnnotationModuleData = srcInfo.getAnnotationModuleData();
                        if (srcDf.isBinary()) {
                            Map<String, InputStream> moduleName2Data = new HashMap<>();
                            moduleName2Data.put(srcAnnotationModuleName, new ByteArrayInputStream(originalAnnotationModuleData));
                            BinaryDecodingResult decode = binaryDecoder.decode(moduleName2Data, ts, reversedBinaryMapping, mappedFeatures, nsMap);
                            originalAnnotationModuleData = binaryXmiBuilder.buildXmi(decode, true).toByteArray();
                        }
                        AdaptedAnnotationModuleData adaptedAnnotationModuleData = adaptAnnotationModuleToTargetDatabase(originalAnnotationModuleData, ts, transferInfo, maxXmiId, targetIdToSofaName);
                        transferData.add(adaptedAnnotationModuleData);
                    } else {
                        FileUtils.write(shaMismatches, targetDocId + "\n", UTF_8, true);
                    }
                }
                transferBatch.clear();

                for (AdaptedAnnotationModuleData adaptedData : transferData) {
                    // TODO and now put everything into the target table:
                    // update table set maxXmiId, new data
                }
            }
        }
        // sha checken; wenn es nicht passt, id in report schreiben
        // hole sofa map in der Ziel-DB
        // xmi ID anpassen
        // sofa anpassen
    }

    /**
     * @param originalAnnotationModuleData The source annotation module data, decoded from binary format, if necessary
     * @param ts
     * @param transferInfo
     * @param maxXmiId
     * @param targetIdToSofaName
     * @return
     */
    private static AdaptedAnnotationModuleData adaptAnnotationModuleToTargetDatabase(byte[] originalAnnotationModuleData, TypeSystem ts, ModuleTransferInfo transferInfo, int maxXmiId, Map<Integer, String> targetIdToSofaName) {
        Map<Integer, String> srcIdToSofaName = transferInfo.getSrcIdToSofaName();
        Map<String, Integer> targetSofaNameToId = targetIdToSofaName.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        StaxXmiSplitter xmiSplitter = new StaxXmiSplitter(Collections.singleton("de.julielab.jcore.types.EmbeddingVector"), true, false, Collections.emptySet());
        try {
            // TODO last time I stopped here the problem was that the XmiSplitter did only worked fine when the sofa element (=basedocument) is loaded itself for the sofa ID mapping.
            // We do not really need the sofa itself but only its ID which exists when we are loading annotations from previously stored annotations in the data table.
            // In the XMI splitter, I already added respective code. However, this has issues when base document is written the first time and no previous sofa id mapping exists.
            // The solution would be do first derive this from the XMI data directly and then proceed as normal. But I haven't done this yet.
            XmiSplitterResult splitterResult = xmiSplitter.process(originalAnnotationModuleData, ts, maxXmiId, srcIdToSofaName.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));
            System.out.println(splitterResult);
        } catch (XMISplitterException e) {
            e.printStackTrace();
        }

//        String xmlString = new String(originalAnnotationModuleData, UTF_8);
//        Matcher sofaMatcher = Pattern.compile("sofa=\"([0-9]+)\"|xmi:id=\"([0-9]+)\"").matcher("");
//        sofaMatcher.reset(xmlString);
//        AtomicInteger maxXmiCounter = new AtomicInteger(maxXmiId);
//        String newXmlString = sofaMatcher.replaceAll(mr -> {
//            if (mr.group().startsWith("sofa"))
//                return "sofa=\"" + targetSofaNameToId.get(srcIdToSofaName.get(Integer.parseInt(mr.group(1)))) + "\"";
//            else
//                return "xmi:id=\"" + maxXmiCounter.incrementAndGet() + "\"";
//        });
//        return new AdaptedAnnotationModuleData(maxXmiCounter.get(), newXmlString);+
        return  null;
    }

    private static class AdaptedAnnotationModuleData {
        private int newMaxXmiId;
        private String adaptedModuleData;

        public int getNewMaxXmiId() {
            return newMaxXmiId;
        }

        public String getAdaptedModuleData() {
            return adaptedModuleData;
        }

        public AdaptedAnnotationModuleData(int newMaxXmiId, String adaptedModuleData) {
            this.newMaxXmiId = newMaxXmiId;
            this.adaptedModuleData = adaptedModuleData;
        }
    }

    private static Map<Integer, String> parseSofaIdMap(byte[] srcDatum, String docId) {
        Map<Integer, String> idToSofaName = new HashMap<>();
        String mappingString = new String(srcDatum, UTF_8);
        String[] mappings = mappingString != null ? mappingString.split("\\|") : null;
        if (mappings != null) {
            for (int i = 0; i < mappings.length; i++) {
                String mapping = mappings[i];
                String[] idAndName = mapping.split("=");
                idToSofaName.put(Integer.parseInt(idAndName[0]), idAndName[1]);
                if (log.isTraceEnabled())
                    log.trace("Retrieved sofa_id_mapping {} for document {}.", Arrays.toString(idAndName), docId);
            }
        }
        return idToSofaName;
    }

    private static String addTargetMaxXmiHashAndSofaMapSchema(String table, String targetTextHashColumnName, boolean targetGzip, boolean targetBinary, DataBaseConnector targetDbc) throws TableSchemaMismatchException, TableNotFoundException {
        List<Map<String, String>> xmiAnnotationColumnsDefinitions = new ArrayList<>();
        xmiAnnotationColumnsDefinitions.add(FieldConfig.createField(JulieXMLConstants.NAME, targetTextHashColumnName, JulieXMLConstants.RETRIEVE, "true", JulieXMLConstants.TYPE, "text"));
        String referenceSchema = targetGzip ? "xmi_text_gzip" : "xmi_text";
        List<Map<String, String>> primaryKey = targetDbc.getActiveTableFieldConfiguration().getPrimaryKeyFields().collect(Collectors.toList());
        FieldConfig xmiSchema = targetDbc.addPKAdaptedFieldConfiguration(primaryKey, referenceSchema, "-maxxmi-hash-sofamap-retrieval", xmiAnnotationColumnsDefinitions);

        checkXmiTableSchema(targetDbc, table, xmiSchema);
        return xmiSchema.getName();
    }

    private static void addTargetColumnsAndXmiSchema(String targetAnnotationModuleName, String targetTextHashColumnName, boolean targetGzip, boolean targetBinary, String table, DataBaseConnector targetDbc) throws TableSchemaMismatchException, TableNotFoundException {
        List<Map<String, String>> xmiAnnotationColumnsDefinitions = new ArrayList<>();
        for (String qualifiedAnnotation : Collections.singletonList(targetAnnotationModuleName)) {
            final String columnName = qualifiedAnnotation.toLowerCase().replace('.', '_').replace(':', '$');
            final Map<String, String> field = FieldConfig.createField(
                    JulieXMLConstants.NAME, columnName,
                    JulieXMLConstants.GZIP, String.valueOf(targetGzip),
                    JulieXMLConstants.RETRIEVE, "true",
                    JulieXMLConstants.TYPE, targetGzip || targetBinary ? "bytea" : "xml"
            );
            xmiAnnotationColumnsDefinitions.add(field);
            targetDbc.assureColumnsExist(table, Collections.singletonList(columnName), field.get(JulieXMLConstants.TYPE));
        }
        xmiAnnotationColumnsDefinitions.add(FieldConfig.createField(JulieXMLConstants.NAME, targetTextHashColumnName, JulieXMLConstants.RETRIEVE, "true", JulieXMLConstants.TYPE, "text"));
        FieldConfig xmiSchema = targetDbc.addXmiTextFieldConfiguration(targetDbc.getActiveTableFieldConfiguration().getPrimaryKeyFields().collect(Collectors.toList()), xmiAnnotationColumnsDefinitions, targetGzip || targetBinary);
        // Deactivate the base document and the max XMI ID from the source table for retrieval, we don't need it. Thus, this position will not be given in the retrieved data.
        xmiSchema.getFields().stream().filter(field -> field.get(JulieXMLConstants.NAME).equals(XmiSplitConstants.BASE_DOC_COLUMN)).findAny().ifPresent(field -> field.put(JulieXMLConstants.RETRIEVE, "false"));
        targetDbc.setActiveTableSchema(xmiSchema.getName());

        checkXmiTableSchema(targetDbc, table, xmiSchema);
    }

    private static void addSrcXmiSchema(String table, String annotationModuleName, String srcTextHashColumnName, DataBaseConnector srcDbc) throws CoStoSysException, TableNotFoundException {
        List<Map<String, String>> primaryKeyFields = srcDbc.getActiveTableFieldConfiguration().getPrimaryKeyFields().collect(Collectors.toList());

        CoStoSysJedisTools.JedisDataFormat df = CoStoSysJedisTools.determineDataFormat(srcDbc, table, annotationModuleName.toLowerCase().replace('.', '_').replace(':', '$'));

        List<Map<String, String>> xmiAnnotationColumnsDefinitions = new ArrayList<>();
        xmiAnnotationColumnsDefinitions.add(FieldConfig.createField(JulieXMLConstants.NAME, srcTextHashColumnName, JulieXMLConstants.RETRIEVE, "true", JulieXMLConstants.TYPE, "text"));
        for (String qualifiedAnnotation : Collections.singletonList(annotationModuleName)) {
            final String columnName = qualifiedAnnotation.toLowerCase().replace('.', '_').replace(':', '$');
            final Map<String, String> field = FieldConfig.createField(
                    JulieXMLConstants.NAME, columnName,
                    JulieXMLConstants.GZIP, String.valueOf(df.isGzip()),
                    JulieXMLConstants.RETRIEVE, "true",
                    JulieXMLConstants.TYPE, df.isGzip() || df.isBinary() ? "bytea" : "xml"
            );
            xmiAnnotationColumnsDefinitions.add(field);
        }
        // The next call creates a new table configuration that bases on the xmi-oriented default configuration. It defines the following columns in the given order:
        // "docid"
        // "base_document"
        // "max_xmi_id"
        // "sofa_mapping"
        // all of which have set 'retrieve' to 'true'. We just added the column(s) for the annotation module(s).
        FieldConfig xmiDocumentTableSchema = srcDbc.addXmiTextFieldConfiguration(primaryKeyFields, xmiAnnotationColumnsDefinitions, df.isGzip());
        // Deactivate the base document and the max XMI ID from the source table for retrieval, we don't need it. Thus, this position will not be given in the retrieved data.
        xmiDocumentTableSchema.getFields().stream().filter(field -> field.get(JulieXMLConstants.NAME).equals(XmiSplitConstants.BASE_DOC_COLUMN)).findAny().ifPresent(field -> field.put(JulieXMLConstants.RETRIEVE, "false"));
        xmiDocumentTableSchema.getFields().stream().filter(field -> field.get(JulieXMLConstants.NAME).equals("max_xmi_id")).findAny().ifPresent(field -> field.put(JulieXMLConstants.RETRIEVE, "false"));
        srcDbc.setActiveTableSchema(xmiDocumentTableSchema.getName());

        checkXmiTableSchema(srcDbc, table, xmiDocumentTableSchema);
    }

    /**
     * Checks if the data table that is read directly or through a subset matches the XMI table schema.
     *
     * @param dbc                    The DBC connected to the correct database.
     * @param tableName              The table name parameter value given to the reader component.
     * @param xmiDocumentTableSchema The XMI document table schema as generated by {@link DataBaseConnector#addXmiTextFieldConfiguration(List, List, boolean)}
     * @throws ResourceInitializationException If the table is no XMI table or SQL errors happen.
     */
    public static void checkXmiTableSchema(DataBaseConnector dbc, String tableName, FieldConfig xmiDocumentTableSchema) throws TableSchemaMismatchException, TableNotFoundException {
        String dataTable = null;
        try {
            dataTable = dbc.getNextOrThisDataTable(tableName);
            dbc.checkTableHasSchemaColumns(dataTable, xmiDocumentTableSchema.getName());
        } catch (TableSchemaMismatchException e) {
            String error;
            if (dbc.isDataTable(tableName))
                error = String.format("The table \"%s\" specified to read  does not match the " +
                        "XMI text storage data schema. Either the DoGzip parameter does not match the setting that " +
                        "was used for the XMI DB Consumer or the specified table is not an XMI table.", tableName);
            else
                error = String.format("The subset table \"%s\" specified to read  " +
                        "references the data table \"%s\". This data table does not match the " +
                        "XMI text storage data schema. Either the DoGzip parameter does not match the setting that " +
                        "was used for the XMI DB Consumer or the specified table is not an XMI table.", tableName, dataTable);
            log.error(error);
            throw new TableSchemaMismatchException(error, e);
        }
    }

    private static class ModuleTransferInfo {
        private String docId;
        private Map<Integer, String> srcIdToSofaName;
        private byte[] srcSha;
        private byte[] annotationModuleData;

        public ModuleTransferInfo(String docId, Map<Integer, String> srcIdToSofaName, byte[] srcSha, byte[] annotationModuleData) {
            this.docId = docId;
            this.srcIdToSofaName = srcIdToSofaName;
            this.srcSha = srcSha;
            this.annotationModuleData = annotationModuleData;
        }

        public String getDocId() {
            return docId;
        }

        public Map<Integer, String> getSrcIdToSofaName() {
            return srcIdToSofaName;
        }

        public byte[] getSrcSha() {
            return srcSha;
        }

        public byte[] getAnnotationModuleData() {
            return annotationModuleData;
        }
    }


}

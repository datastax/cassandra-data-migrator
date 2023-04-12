package datastax.cdm.properties;

import datastax.cdm.job.MigrateDataType;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class PropertyHelperTest_ColumnsKeysTypes {
    PropertyHelper helper;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    public void minimum() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"a,b");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "a");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,2");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "a");
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("a","b"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("2")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Collections.singletonList("a"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(helper.getOriginColumnNames(), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(helper.getOriginColumnTypes(), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(helper.getTargetPKNames(), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0,1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"a", "a"}, {"b", "b"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                                  ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void minimum_moreComplex() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES, "part1,part2,key1,val1,val2");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "part1,part2");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES, "2,1,0,3,3");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "part1,part2,key1");
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("part1", "part2", "key1", "val1", "val2"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("2"), new MigrateDataType("1"), new MigrateDataType("0"), new MigrateDataType("3"), new MigrateDataType("3")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList("part1", "part2", "key1"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("2"), new MigrateDataType("1"), new MigrateDataType("0")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(helper.getOriginColumnNames(), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(helper.getOriginColumnTypes(), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(helper.getTargetPKNames(), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0, 1, 2, 3, 4), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"part1", "part1"}, {"part2", "part2"}, {"key1", "key1"}, {"val1", "val1"}, {"val2", "val2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        , helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void minimum_moreComplex_changeTargetPKOrder() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES, "part1,part2,key1,val1,val2");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "part1,part2");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES, "2,1,0,3,3");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "key1,part1,part2");
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("part1", "part2", "key1", "val1", "val2"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("2"), new MigrateDataType("1"), new MigrateDataType("0"), new MigrateDataType("3"), new MigrateDataType("3")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList("key1", "part1", "part2"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("0"), new MigrateDataType("2"), new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(helper.getOriginColumnNames(), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(helper.getOriginColumnTypes(), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(helper.getTargetPKNames(), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0, 1, 2, 3, 4), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"part1", "part1"}, {"part2", "part2"}, {"key1", "key1"}, {"val1", "val1"}, {"val2", "val2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        , helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void minimum_moreComplex_columnNamesOutOfOrder() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES, "val2,val1,key1,part2,part1");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "part1,part2");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES, "3,3,0,1,2");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "key1,part1,part2");
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("val2","val1","key1","part2","part1"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("3"), new MigrateDataType("3"), new MigrateDataType("0"), new MigrateDataType("1"), new MigrateDataType("2")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList("key1","part1", "part2"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("0"), new MigrateDataType("2"), new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(helper.getOriginColumnNames(), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(helper.getOriginColumnTypes(), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(helper.getTargetPKNames(), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0, 1, 2, 3, 4), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"part1", "part1"}, {"part2", "part2"}, {"key1", "key1"}, {"val1", "val1"}, {"val2", "val2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        , helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void renameColumns_minimum() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"a,b");
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET,"a:A,b:B");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "a");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,2");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "A");
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("a","b"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("2")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Collections.singletonList("A"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("A","B"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(helper.getOriginColumnTypes(), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0,1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"a", "A"}, {"b", "B"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap"),
                () -> assertEquals(Stream.of(new String[][]{{"A", "a"}, {"B", "b"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getTargetColumnName_OriginColumnNameMap(), "getTargetColumnName_OriginColumnNameMap")

        );
    }

    @Test
    public void renameColumns_complex() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES, "\"part-1\",\"part-2\",\"key-1\",\"val-1\",\"val-2\"");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "\"part-1\",\"part-2\"");
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET,"\"part-1\":part1,\"part-2\":part2,\"key-1\":key1,\"val-1\":val1,\"val-2\":val2");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES, "3,2,1,7,9");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "part1,part2,key1");
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("\"part-1\"","\"part-2\"","\"key-1\"","\"val-1\"","\"val-2\""), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("3"), new MigrateDataType("2"), new MigrateDataType("1"), new MigrateDataType("7"), new MigrateDataType("9")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("\"part-1\"","\"part-2\""), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList("part1","part2","key1"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("3"), new MigrateDataType("2"), new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("part1","part2","key1","val1","val2"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(helper.getOriginColumnTypes(), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(Arrays.asList("\"part-1\"","\"part-2\"","\"key-1\""), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0, 1, 2, 3, 4), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"\"part-1\"", "part1"}, {"\"part-2\"", "part2"}, {"\"key-1\"", "key1"}, {"\"val-1\"", "val1"}, {"\"val-2\"", "val2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        , helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void shuffleTargetNames_dropOne() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES, "part1,part2,key1,val1,val2,drop1");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES, "9,7,1,2,3,4");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "part1,part2");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "part1,part2,key1");
        sc.set(KnownProperties.TARGET_COLUMN_NAMES, "val2,val1,key1,part2,part1"); // reverse order, drop1 is dropped
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("part1", "part2", "key1", "val1", "val2", "drop1"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("9"), new MigrateDataType("7"), new MigrateDataType("1"), new MigrateDataType("2"), new MigrateDataType("3"), new MigrateDataType("4")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList("part1", "part2", "key1"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Arrays.asList("val2", "val1", "key1", "part2", "part1"), helper.getTargetColumnNames(), "targetColumnNames"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("9"), new MigrateDataType("7"), new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("3"), new MigrateDataType("2"), new MigrateDataType("1"), new MigrateDataType("7"), new MigrateDataType("9")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2", "key1"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("9"), new MigrateDataType("7"), new MigrateDataType("1")), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(4, 3, 2, 1, 0), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes")
        );
    }

    @Test
    public void basic_specifyEverything() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES, "part1,part2,key1,val1,val2,drop1");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES, "9,7,1,2,3,4");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "part1,part2");
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET, "part1:part_1,part2:part_2,key1:key_1,val1:val_1,val2:val_2");
        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "part_1,part_2,key_1");
        sc.set(KnownProperties.TARGET_COLUMN_NAMES, "part_1,part_2,key_1,val_1,val_2"); // drop1 is dropped
        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("part1", "part2", "key1", "val1", "val2", "drop1"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("9"), new MigrateDataType("7"), new MigrateDataType("1"), new MigrateDataType("2"), new MigrateDataType("3"), new MigrateDataType("4")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList("part_1", "part_2", "key_1"), helper.getTargetPKNames(), "getTargetPKNames"),
                () -> assertEquals(Arrays.asList("part_1", "part_2", "key_1", "val_1", "val_2"), helper.getTargetColumnNames(), "targetColumnNames"),
                () -> assertEquals(Stream.of(new String[][]{{"part1", "part_1"}, {"part2", "part_2"}, {"key1", "key_1"}, {"val1", "val_1"}, {"val2", "val_2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        , helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("9"), new MigrateDataType("7"), new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("9"), new MigrateDataType("7"), new MigrateDataType("1"), new MigrateDataType("2"), new MigrateDataType("3")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),
                () -> assertEquals(Arrays.asList("part1", "part2", "key1"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(helper.getTargetPKTypes(), helper.getOriginPKTypes(), "getOriginPKTypes"),
                () -> assertEquals(Arrays.asList(0, 1, 2, 3, 4), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes")
        );
    }

    @Test
    public void constantColumns() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"a,b");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "a");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,2");

        sc.set(KnownProperties.CONSTANT_COLUMN_NAMES, "const1,const2");
        sc.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abc',123");
        sc.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0,1");

        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "a,const1");

        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("a","b"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("2")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("a","const1"), helper.getTargetPKNames(), "getTargetPKNames"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("1"),new MigrateDataType("0")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("a","b","const1","const2"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("2"), new MigrateDataType("0"), new MigrateDataType("1")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),

                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getOriginPKTypes(), "getOriginPKTypes"),

                () -> assertEquals(Arrays.asList(0,1,-1,-1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"a", "a"}, {"b", "b"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void constantColumns_specifiedTargetNames() {
        // Constant names should be moved to the end of the name list
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"a,b");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "a");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,2");

        sc.set(KnownProperties.CONSTANT_COLUMN_NAMES, "const1,const2");
        sc.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abc',123");
        sc.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0,1");

        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "a,const1");
        sc.set(KnownProperties.TARGET_COLUMN_NAMES, "const1,const2,a,b");

        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("a","b"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("2")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("a","const1"), helper.getTargetPKNames(), "getTargetPKNames"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("1"),new MigrateDataType("0")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("a","b","const1","const2"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("2"), new MigrateDataType("0"), new MigrateDataType("1")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),

                () -> assertEquals(Collections.singletonList("a"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getOriginPKTypes(), "getOriginPKTypes"),

                () -> assertEquals(Arrays.asList(0,1,-1,-1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"a", "a"}, {"b", "b"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void explodeMap_basic() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"key1,map1");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "key1");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,5%1%4");

        sc.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map1");
        sc.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "map_key");
        sc.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "map_value");

        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "key1,map_key");

        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("key1","map1"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Collections.singletonList("key1"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("5%1%4")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("key1","map_key"), helper.getTargetPKNames(), "getTargetPKNames"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("1"),new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("key1","map_key","map_value"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("1"), new MigrateDataType("4")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),

                () -> assertEquals(Collections.singletonList("key1"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getOriginPKTypes(), "getOriginPKTypes"),

                () -> assertEquals(Arrays.asList(0,-1,-1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"key1", "key1"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    @Test
    public void explodeMap_otherColumns() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"key1,value1,map1,value2");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "key1");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,4,5%1%4,3");

        sc.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "map1");
        sc.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "map_key");
        sc.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "map_value");

        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "key1,map_key");

        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("key1","value1","map1","value2"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Collections.singletonList("key1"), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("4"), new MigrateDataType("5%1%4"), new MigrateDataType("3")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("key1","map_key"), helper.getTargetPKNames(), "getTargetPKNames"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("1"),new MigrateDataType("1")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("key1","value1","value2","map_key","map_value"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("4"), new MigrateDataType("3"), new MigrateDataType("1"), new MigrateDataType("4")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),

                () -> assertEquals(Collections.singletonList("key1"), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getOriginPKTypes(), "getOriginPKTypes"),

                () -> assertEquals(Arrays.asList(0,1,3,-1,-1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"key1", "key1"}, {"value1","value1"}, {"value2","value2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }

    // This test something of a regression test, combining lots of different elements.
    // It is somewhat testing complexity of CDM-34.
    @Test
    public void explodeMap_constantColumn_otherColumns_rename() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES,"\"key-1\",\"value-1\",\"map-1\",value2");
        sc.set(KnownProperties.ORIGIN_PARTITION_KEY, "\"key-1\"");
        sc.set(KnownProperties.ORIGIN_COLUMN_TYPES,"1,4,5%2%4,3");

        sc.set(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET,"\"key-1\":key1,\"value-1\":value1");

        sc.set(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME, "\"map-1\"");
        sc.set(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME, "map_key");
        sc.set(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME, "map_value");

        sc.set(KnownProperties.CONSTANT_COLUMN_NAMES, "const1,const2");
        sc.set(KnownProperties.CONSTANT_COLUMN_VALUES, "'abc',123");
        sc.set(KnownProperties.CONSTANT_COLUMN_TYPES, "0,1");

        sc.set(KnownProperties.TARGET_PRIMARY_KEY, "const1,const2,key1,map_key");

        helper.initializeSparkConf(sc);

        assertAll(
                () -> assertEquals(Arrays.asList("\"key-1\"","\"value-1\"","\"map-1\"","value2"), helper.getOriginColumnNames(), "originColumnNames"),
                () -> assertEquals(Collections.singletonList("\"key-1\""), helper.getOriginPartitionKeyNames(), "getOriginPartitionKeyNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("4"), new MigrateDataType("5%2%4"), new MigrateDataType("3")), helper.getOriginColumnTypes(), "getOriginColumnTypes"),
                () -> assertEquals(Arrays.asList("const1","const2","key1","map_key"), helper.getTargetPKNames(), "getTargetPKNames"),

                () -> assertEquals(Arrays.asList(new MigrateDataType("0"),new MigrateDataType("1"),new MigrateDataType("1"),new MigrateDataType("2")), helper.getTargetPKTypes(), "getTargetPKTypes"),
                () -> assertEquals(Arrays.asList("key1","value1","value2","map_key","map_value","const1","const2"), helper.getTargetColumnNames(), "getTargetColumnNames"),
                () -> assertEquals(Arrays.asList(new MigrateDataType("1"), new MigrateDataType("4"), new MigrateDataType("3"), new MigrateDataType("2"), new MigrateDataType("4"),new MigrateDataType("0"), new MigrateDataType("1")), helper.getTargetColumnTypes(), "getTargetColumnTypes"),

                () -> assertEquals(Collections.singletonList("\"key-1\""), helper.getOriginPKNames(), "getOriginPKNames"),
                () -> assertEquals(Collections.singletonList(new MigrateDataType("1")), helper.getOriginPKTypes(), "getOriginPKTypes"),

                () -> assertEquals(Arrays.asList(0,1,3,-1,-1,-1,-1), helper.getTargetToOriginColumnIndexes(), "getTargetToOriginColumnIndexes"),
                () -> assertEquals(Stream.of(new String[][]{{"\"key-1\"", "key1"}, {"\"value-1\"","value1"}, {"value2","value2"}}).collect(Collectors.toMap(data -> data[0], data -> data[1]))
                        ,helper.getOriginColumnName_TargetColumnNameMap(), "getOriginColumnName_TargetColumnNameMap")
        );
    }


}
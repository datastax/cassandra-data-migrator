package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Properties;

public class NoSparkMigrate {


    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));
        System.setProperties(props);

        String splitSize = System.getProperty("spark.migrate.splitSize","10000");
        BigInteger minPartition = new BigInteger(System.getProperty("spark.migrate.source.minPartition"));
        BigInteger maxPartition = new BigInteger(System.getProperty("spark.migrate.source.maxPartition"));
        Collection<SplitPartitions.Partition> partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition, 100);

/*
        partitions.parallelStream().forEach( part ->
                 CopyJobSession.getInstance(sourceSession,astraSession, sc.getConf).getDataAndInsert(part.getMin(), part.getMax())
        );

 */
//        parts.foreach(part => {
//                sourceConnection.withSessionDo(sourceSession => astraConnection.withSessionDo(astraSession=>   CopyJobSession.getInstance(sourceSession,astraSession, sc.getConf).getDataAndInsert(part.getMin, part.getMax)))


    }

    private static Session createAstraSession(String scb, String username, String password){
        return CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(scb))
                .withAuthCredentials(username,password)
                .build();
    }

    private static Session createSession(String hostname, String username, String password){
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostname,9042) )
                .withAuthCredentials(username,password)
                .build();
    }
}

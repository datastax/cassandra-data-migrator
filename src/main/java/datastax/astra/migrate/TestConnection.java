package datastax.astra.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.ConnectException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.stream.Stream;

public class TestConnection {


    public static void main(String[] args) {



            try {

//                SSLContext sslContext = createSslContext(new FileInputStream("/Users/ankitpatel/Documents/Astra/secure-connect-enterprise/identity.jks"), "EZ65kF7s8jpc9vriS".toCharArray(),
//                        new FileInputStream("/Users/ankitpatel/Documents/Astra/secure-connect-enterprise/trustStore.jks"), "CeH6v7cXD1u8wNI9V".toCharArray());


                SSLContext sslContext = createSslContext(new FileInputStream("/Users/ankitpatel/Documents/Astra/priceline/b/identity.jks"), "46K5lMPCXS8Hg7IhJ".toCharArray(),
                        new FileInputStream("/Users/ankitpatel/Documents/Astra/priceline/b/trustStore.jks"), "V8S9wRIM0E7z2TDqU".toCharArray());


//                HttpsURLConnection connection = (HttpsURLConnection)new URL("https://ca91bcb3-1fe4-4fd4-abd1-33deca9a88ab-us-east1.db.astra.datastax.com:29080/metadata").openConnection();
                HttpsURLConnection connection = (HttpsURLConnection)new URL("https://c0f6d2d4-4418-4603-8e0b-1860a5746f7c-us-east4.db.astra.datastax.com:29080/metadata").openConnection();

                connection.setSSLSocketFactory(sslContext.getSocketFactory());
                connection.setRequestMethod("GET");
                connection.setRequestProperty("host", "localhost");

                Stream<String> lines = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)).lines();
                lines.forEach(line -> System.out.println(line));
            } catch (ConnectException var4) {
                throw new IllegalStateException("Unable to connect to cloud metadata service. Please make sure your cluster is not parked or terminated", var4);
            } catch (Exception var5) {
                throw new IllegalStateException("Unable to resolve host for cloud metadata service. Please make sure your cluster is not terminated", var5);
            }


if(false){
    return;
}

        // Create the CqlSession object:
        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get("/Users/ankitpatel/Documents/Astra/albertsons/secure-connect-enterprise.zip"))
                .withAuthCredentials("GbqYadICWAUUWWZGHFfAvCMi","cG5ceaj3AIPm.gzSXT0msfrfZgibry3l+pE+mHHsQRXS-05kztIrmlvg8El1448jYp,cJlyXDaaD53yFXTbYYEuOMsvQ-bU0HWfd.t9iF-SvBmS2pM5Hu4W2CkHM45Pd")
                .build()) {
            // Select the release_version from the system.local table:
            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            //Print the results of the CQL query to the console:
            if (row != null) {
                System.out.println("Release Version" + row.getString("release_version"));
            } else {
                System.out.println("An error occurred.");
            }
        }
        System.exit(0);
    }


    @NonNull
    protected static SSLContext createSslContext(@NonNull InputStream keyStoreInputStream, @NonNull char[] keyStorePassword, @NonNull InputStream trustStoreInputStream, @NonNull char[] trustStorePassword) throws IOException, GeneralSecurityException {
        KeyManagerFactory kmf = createKeyManagerFactory(keyStoreInputStream, keyStorePassword);
        TrustManagerFactory tmf = createTrustManagerFactory(trustStoreInputStream, trustStorePassword);
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return sslContext;
    }

    @NonNull
    protected static KeyManagerFactory createKeyManagerFactory(@NonNull InputStream keyStoreInputStream, @NonNull char[] keyStorePassword) throws IOException, GeneralSecurityException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(keyStoreInputStream, keyStorePassword);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyStorePassword);
        Arrays.fill(keyStorePassword, '\u0000');
        return kmf;
    }

    @NonNull
    protected static TrustManagerFactory createTrustManagerFactory(@NonNull InputStream trustStoreInputStream, @NonNull char[] trustStorePassword) throws IOException, GeneralSecurityException {
        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(trustStoreInputStream, trustStorePassword);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        Arrays.fill(trustStorePassword, '\u0000');
        return tmf;
    }

}

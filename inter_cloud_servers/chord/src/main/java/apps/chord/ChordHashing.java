package apps.chord;

import java.util.List;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException; 
import java.lang.Math;
import java.math.BigInteger;

public class ChordHashing {

    public static long hash_node(ChordNode node, int M) {
        if(node == null) return -1;

        return hash_ip(node.get_ip(), node.get_m_port(), M);
    }

    public static long hash_ip(String ip, int port, int M) {
        if(ip == null) return -1;

        String key = ip + ":"+port;
        return hash_key(key, M);
    }

    public static long hash_key(String key, int M) {
        if(key == null || M < 0) return -1;

        try{
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] messageDigest = md.digest(key.getBytes());
            long hashLong = new BigInteger(1, messageDigest).longValue();
            return Math.abs(hashLong % (long) Math.pow(2, M));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


}

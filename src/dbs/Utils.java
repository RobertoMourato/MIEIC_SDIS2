package dbs;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

class Utils {
    static Long hash_id_peer(String address, Integer port) {
        String id = address + port.toString();
        MessageDigest md = null;

        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        assert md != null;
        byte[] hash = md.digest(id.getBytes());

        md.update(id.getBytes(), 0, id.length());
        BigInteger h = new BigInteger(1, md.digest());


        BigInteger bi = BigInteger.valueOf((1L << ChordNode.m));
        bi = bi.subtract(BigInteger.ONE);

        bi = bi.and(h);

        return bi.longValue();
    }

    private static Long hash_id_file(File file) {
        Path path = Paths.get(file.getAbsolutePath());

        try {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);

            String msg = file.getName() + attributes.lastModifiedTime();

            MessageDigest digest = MessageDigest.getInstance("SHA-1");

            BigInteger h = new BigInteger(1, digest.digest(msg.getBytes(StandardCharsets.UTF_8)));

            BigInteger bi = BigInteger.valueOf((1L << ChordNode.m));
            bi = bi.subtract(BigInteger.ONE);

            bi = bi.and(h);

            return bi.longValue();

        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return -1L;
    }

    static List<Long> hashes_of_file(File file, int replicationDegree) {
        List<Long> list_ids = new ArrayList<>();

        Long originalId = hash_id_file(file);

        if (originalId == -1)
            return  list_ids;

        long maxNumber = 1L << ChordNode.m;

        Long portionMaxNumber = maxNumber / 9;

        List<Integer> positions = positionsForReplicationDegree(replicationDegree);

        for (Integer position : positions) {
            Long cur = (originalId + (portionMaxNumber * position)) % maxNumber;
            list_ids.add(cur);
        }

        return list_ids;
    }

    static List<Integer> positionsForReplicationDegree(int replicationDegree){
        List<Integer> positions = new ArrayList<>();
        positions.add(0);

        switch (replicationDegree){
            case 1:
                return positions;
            case 2:
                positions.add(4);
                return positions;
            case 3:
                positions.add(3);
                positions.add(6);
                return positions;
            case 4:
                positions.add(2);
                positions.add(4);
                positions.add(6);
                return positions;
            case 5:
                positions.add(2);
                positions.add(4);
                positions.add(5);
                positions.add(7);
                return positions;
            case 6:
                positions.add(1);
                positions.add(3);
                positions.add(4);
                positions.add(6);
                positions.add(7);
                return positions;
            case 7:
                positions.add(1);
                positions.add(2);
                positions.add(4);
                positions.add(5);
                positions.add(7);
                positions.add(8);
                return positions;
            case 8:
                positions.add(1);
                positions.add(2);
                positions.add(3);
                positions.add(4);
                positions.add(6);
                positions.add(7);
                positions.add(8);
                return positions;
            case 9:
                positions.add(1);
                positions.add(2);
                positions.add(3);
                positions.add(4);
                positions.add(5);
                positions.add(6);
                positions.add(7);
                positions.add(8);
                return positions;
            default:
                return positions;
        }
    }
}
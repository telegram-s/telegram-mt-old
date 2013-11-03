package org.telegram.mtproto.secure;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;

/**
 * Author: Korshakov Stepan
 * Created: 18.07.13 3:54
 */
public class CryptoUtils {
    public static byte[] RSA(byte[] src, BigInteger key, BigInteger exponent) {
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PublicKey publicKey = keyFactory.generatePublic(new RSAPublicKeySpec(key, exponent));
            Cipher cipher = Cipher.getInstance("RSA/ECB/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher.doFinal(src);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] AES256IGEDecrypt(byte[] src, byte[] iv, byte[] key) {
        AESFastEngine engine = new AESFastEngine();
        engine.init(false, new KeyParameter(key));

        int blocksCount = src.length / 16;
        byte[] outData = new byte[src.length];

        byte[] curIvX = iv;
        byte[] curIvY = iv;
        int curIvXOffset = 16;
        int curIvYOffset = 0;

        for (int i = 0; i < blocksCount; i++) {
            int offset = i * 16;

            for (int j = 0; j < 16; j++) {
                outData[offset + j] = (byte) (src[offset + j] ^ curIvX[curIvXOffset + j]);
            }
            engine.processBlock(outData, offset, outData, offset);
            for (int j = 0; j < 16; j++) {
                outData[offset + j] = (byte) (outData[offset + j] ^ curIvY[curIvYOffset + j]);
            }

            curIvY = src;
            curIvYOffset = offset;
            curIvX = outData;
            curIvXOffset = offset;
        }

        return outData;
    }

    public static void AES256IGEDecrypt(File src, File dest, byte[] iv, byte[] key) throws IOException {
        AESFastEngine engine = new AESFastEngine();
        engine.init(false, new KeyParameter(key));

        byte[] curIvX = substring(iv, 16, 16);
        byte[] curIvY = substring(iv, 0, 16);

        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(src));
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dest));
        byte[] buffer = new byte[16];
        int count;
        while ((count = inputStream.read(buffer)) > 0) {
            byte[] outData = new byte[16];
            for (int j = 0; j < 16; j++) {
                outData[j] = (byte) (buffer[j] ^ curIvX[j]);
            }
            engine.processBlock(outData, 0, outData, 0);
            for (int j = 0; j < 16; j++) {
                outData[j] = (byte) (outData[j] ^ curIvY[j]);
            }

            curIvY = buffer;
            curIvX = outData;
            buffer = new byte[16];

            outputStream.write(outData);
        }
        outputStream.flush();
        outputStream.close();
        inputStream.close();
    }

    public static void AES256IGEEncrypt(File src, File dest, byte[] iv, byte[] key) throws IOException {
        AESFastEngine engine = new AESFastEngine();
        engine.init(true, new KeyParameter(key));

        byte[] curIvX = substring(iv, 16, 16);
        byte[] curIvY = substring(iv, 0, 16);

        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(src));
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dest));
        byte[] buffer = new byte[16];
        int count;
        while ((count = inputStream.read(buffer)) > 0) {
            byte[] outData = new byte[16];
            for (int j = 0; j < 16; j++) {
                outData[j] = (byte) (buffer[j] ^ curIvY[j]);
            }
            engine.processBlock(outData, 0, outData, 0);
            for (int j = 0; j < 16; j++) {
                outData[j] = (byte) (outData[j] ^ curIvX[j]);
            }

            curIvX = buffer;
            curIvY = outData;
            buffer = new byte[16];

            outputStream.write(outData);
        }
        outputStream.flush();
        outputStream.close();
        inputStream.close();
    }

    public static byte[] AES256IGEEncrypt(byte[] src, byte[] iv, byte[] key) {
        AESFastEngine engine = new AESFastEngine();
        engine.init(true, new KeyParameter(key));

        int blocksCount = src.length / 16;
        byte[] outData = new byte[src.length];

        byte[] curIvX = iv;
        byte[] curIvY = iv;
        int curIvXOffset = 16;
        int curIvYOffset = 0;

        for (int i = 0; i < blocksCount; i++) {

            int offset = i * 16;

            for (int j = 0; j < 16; j++) {
                outData[offset + j] = (byte) (src[offset + j] ^ curIvY[curIvYOffset + j]);
            }
            engine.processBlock(outData, offset, outData, offset);
            for (int j = 0; j < 16; j++) {
                outData[offset + j] = (byte) (outData[offset + j] ^ curIvX[curIvXOffset + j]);
            }

            curIvX = src;
            curIvXOffset = offset;
            curIvY = outData;
            curIvYOffset = offset;
        }

        return outData;
    }

//    public static String base64(byte[] data) {
//        return Base64.encodeToString(data, Base64.DEFAULT);
//    }

    public static String MD5(byte[] src) {
        try {
            MessageDigest crypt = MessageDigest.getInstance("MD5");
            crypt.reset();
            crypt.update(src);
            return ToHex(crypt.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static byte[] MD5Raw(byte[] src) {
        try {
            MessageDigest crypt = MessageDigest.getInstance("MD5");
            crypt.reset();
            crypt.update(src);
            return crypt.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String ToHex(byte[] src) {
        String res = "";
        for (int i = 0; i < src.length; i++) {
            res += String.format("%02X", src[i] & 0xFF);
        }
        return res.toLowerCase();
    }

    public static byte[] SHA1(byte[] src) {
        try {
            MessageDigest crypt = MessageDigest.getInstance("SHA-1");
            crypt.reset();
            crypt.update(src);
            return crypt.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static byte[] SHA1(byte[]... src1) {
        try {
            MessageDigest crypt = MessageDigest.getInstance("SHA-1");
            crypt.reset();
            for (int i = 0; i < src1.length; i++) {
                crypt.update(src1[i]);
            }
            return crypt.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static boolean arrayEq(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }

    public static byte[] concat(byte[]... v) {
        int len = 0;
        for (int i = 0; i < v.length; i++) {
            len += v[i].length;
        }
        byte[] res = new byte[len];
        int offset = 0;
        for (int i = 0; i < v.length; i++) {
            System.arraycopy(v[i], 0, res, offset, v[i].length);
            offset += v[i].length;
        }
        return res;
    }

    public static byte[] substring(byte[] src, int start, int len) {
        byte[] res = new byte[len];
        System.arraycopy(src, start, res, 0, len);
        return res;
    }

    public static byte[] align(byte[] src, int factor) {
        if (src.length % factor == 0) {
            return src;
        }
        int padding = factor - src.length % factor;

        return concat(src, Entropy.generateSeed(padding));
    }

    public static byte[] alignKeyZero(byte[] src, int size) {
        if (src.length == size) {
            return src;
        }

        if (src.length > size) {
            return substring(src, src.length - size, size);
        } else {
            return concat(new byte[size - src.length], src);
        }
    }

    public static byte[] xor(byte[] a, byte[] b) {
        byte[] res = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            res[i] = (byte) (a[i] ^ b[i]);
        }
        return res;
    }

    public static BigInteger loadBigInt(byte[] data) {
        return new BigInteger(1, data);
    }

    public static byte[] fromBigInt(BigInteger val) {
        byte[] res = val.toByteArray();
        if (res[0] == 0) {
            byte[] res2 = new byte[res.length - 1];
            System.arraycopy(res, 1, res2, 0, res2.length);
            return res2;
        } else {
            return res;
        }
    }
}
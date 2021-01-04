package utils.id;

import it.unimi.dsi.util.XorShift1024StarPhiRandom;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

/**
 * Use advanced XORShift generators for generation. More at http://xorshift.di.unimi.it/
 * The Seed for Random generation MUST have enough entropy and is best taken from the System.
 * More at http://www.javamex.com/tutorials/random_numbers/seeding_entropy.shtml
 */

public class IDGenerator {

  private static Random singletonRandom1024;

  private static boolean initialized = false;

  private final int bitLength;

  public IDGenerator(int bitLength) {
    initialize();
    this.bitLength = bitLength;
  }

  protected void reSeed(){
    seed(singletonRandom1024);
  }

  private static void seed(final Random random) {
    SecureRandom secureRandom = new SecureRandom();
    byte[] rand = secureRandom.generateSeed(8);
    ByteBuffer buffer = ByteBuffer.wrap(rand);
    long seed = buffer.getLong();
    // Ensure Random is locked
    synchronized (random) {
      random.setSeed(seed);
    }
  }

  // Depend on OS given entropy.
  /**
   * @param random
   */
  private static void seedGenerator(final Random random) {
    seed(random);
  }


  /**
   * Initializes the Random objects safely and seeds it up!
   */
  private static synchronized void initialize() {
    if (initialized) {
      return;
    }
    singletonRandom1024 = new XorShift1024StarPhiRandom();
    seedGenerator(singletonRandom1024);
    initialized = true;
  }


  /**
   * Internal function that will lock and create a
   *
   * @param bitLength
   * @return
   */
  private static BigInteger getRandomInteger(int bitLength) {
    final int bits = Math.max(bitLength, 64);
    // The Random object is not thread-safe
    synchronized (singletonRandom1024) {
      return new BigInteger(bits, singletonRandom1024);
    }
  }



  /**
   * @return The next random ID  with given fixed length in Base36. ID will be in Uppercase by default.
   */
  public String nextID() {
    return (nextValue().toString(36).toUpperCase());
  }

  /**
   * Make this thread safe as underlying Random Object isn't
   *
   * @return the ID of given length
   */
  protected BigInteger nextValue() {
    return getRandomInteger(bitLength);
  }

  public static void main(String[] args) throws Exception {
    final int maxIterations = (null != args && args.length > 0) ? Integer.parseInt(args[0]) : 100000000;
    IDGenerator idCaseInsenstive = new IDGenerator(73); // Just to get a random ID of average 14 characters long
    int maxlenIdCaseInsenstive  = 0,minlenIdCaseInsenstive=Integer.MAX_VALUE;
    FileWriter writer1 = new FileWriter("caseInsensitive.txt");
    try(PrintWriter pw1 = new PrintWriter(writer1)) {
      for (int i = 0; i < maxIterations; i++) {
        String idCaseSenstiveChars = idCaseInsenstive.nextID();
        pw1.printf("%s%s",idCaseSenstiveChars, System.lineSeparator());
        maxlenIdCaseInsenstive = Math.max(maxlenIdCaseInsenstive,idCaseSenstiveChars.length());
        minlenIdCaseInsenstive = Math.min(minlenIdCaseInsenstive,idCaseSenstiveChars.length());
      }
    }
    System.out.println("minlenIdCaseInsenstive="+minlenIdCaseInsenstive+", maxlenIdCaseInsenstive="+maxlenIdCaseInsenstive);

  }

}



package utils.id;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.*;


public class IDGeneratorMultiThreadedTest {

  private static class Params {
    final int bitLength;
    final String outFileName;

    public Params(int bitLength, String outFileName) {
      this.bitLength = bitLength;
      this.outFileName = outFileName;
    }
  }

  private void test(final int threadCount, final int test_size, final Params params) throws InterruptedException, ExecutionException {
    final Set<String> st = new ConcurrentSkipListSet<String>();
    //final IDGenerator idGenerator = new IDGenerator(bitLength,charLength);

    Callable<Long> task = new Callable<Long>() {
      @Override
      public Long call() {
        final IDGenerator idGenerator = new IDGenerator(params.bitLength);
        long start = System.currentTimeMillis();
        String previous = null;
        int distance = 0;
        int resetSeedCount = 0;
        for (int i = 0; i < test_size; i++) {
          String rs = idGenerator.nextID();

          if (!st.add(rs))
            throw new RuntimeException("**** DUPLICATE" + rs);
          if (i > 0 && i == (test_size / 2)) {
            idGenerator.reSeed();
            resetSeedCount++;
          }
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("Min Distance =" + minD + ", Max Distance=" + maxD + ". Seed Reset Count=" + resetSeedCount + ". Time =" + time + " ms, by" + Thread.currentThread().getName());
        return time;
      }
    };
    List<Callable<Long>> tasks = Collections.nCopies(threadCount, task);
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    List<Future<Long>> futures = executorService.invokeAll(tasks);
    Long totalTime = 0l;
    // Check for exceptions
    for (Future<Long> future : futures) {
      // Throws an exception if an exception was thrown by the task.
      totalTime += future.get();
    }
    executorService.shutdownNow();

    System.out.println("Created GUIDs=" + (st.size()) + ", in total time in milliseconds=" + totalTime + ", In seconds=" + (totalTime / 1000) + ".");
    // Validate the IDs
    if (threadCount * test_size != st.size()) {
      throw new RuntimeException(String.format("Expected count %d but found %d", threadCount * test_size, st.size()));
    }

    //writeToFile(st,params.outFileName);
    //Comment Livenshtein
    analyzeIDs(st, Math.max(10, threadCount * 5), 5);
    System.out.println("Completed Test MinDistance=" + minD + ", MaxDistance=" + maxD);

  }

  private static final String FILE_ID_STORE = System.getProperty("java.io.tmpdir") + File.separator;

  private void writeToFile(Set<String> st, String outFile) {
    try {
      final File file = new File(FILE_ID_STORE + outFile);

      if (!file.exists() && !file.createNewFile()) {
        throw new RuntimeException("Failed to create the file");
      }
      try (BufferedWriter out = new BufferedWriter(new FileWriter(file, true));){
        for (String id : st) {
          out.write(id + "\n");
        }
      }
      System.out.println("Finished writing the file..." + file);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void analyzeIDs(Set<String> st, int threadCount, int levenshteinThreshold) {

    final String guids[] = st.toArray(new String[0]);
    long start = System.currentTimeMillis();
    List<Callable<Long>> tasks = new ArrayList<>();
    for (int t = 0; t < threadCount; t++) {
      LevenshteinAnalysis l = new LevenshteinAnalysis(guids, t, threadCount, levenshteinThreshold);
      tasks.add(l);
    }
    Long totalComparisons = 0l;
    try {
      ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
      List<Future<Long>> futures = executorService.invokeAll(tasks);
      // Check for exceptions
      for (Future<Long> future : futures) {
        // Throws an exception if an exception was thrown by the task.
        totalComparisons += future.get();
      }
      executorService.shutdownNow();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Analyze  GUIDs=  of total= " + (st.size()) + ", TotalComparisons =" + totalComparisons + ". Time taken=" + (System.currentTimeMillis() - start));
  }

  static boolean inSample() {
    if (Math.random() * 100 <= 0.1) {
      // picking 0.1 % sample
      return true;
    } else {
      return false;
    }
    //return true;
  }

  static  int minD = Integer.MIN_VALUE;
  static  int maxD = Integer.MAX_VALUE;

  static class LevenshteinAnalysis implements Callable<Long> {
    final String[] guids;
    final int offset, numThreads, threshold;
    final LevenshteinDistance levenshteinDistance;

    public LevenshteinAnalysis(String[] guids, int offset, int numThreads, int threshold) {
      this.guids = guids;
      this.offset = offset;
      this.numThreads = numThreads;
      this.threshold = threshold;
      this.levenshteinDistance = new LevenshteinDistance(threshold);
    }

    @Override
    public Long call() {
      long start = System.currentTimeMillis();
      int counter = offset;
      long processed = 0;
      int minThreadDistance = Integer.MIN_VALUE;
      int maxThreadDistance = Integer.MAX_VALUE;
      while (counter < (guids.length * guids.length)) {
        int i = counter / guids.length;
        int j = counter % guids.length;
        if (j > i && inSample()) {
          processed++;
          if (processed > 0 && processed % 1000000 == 0) {
            System.out.println(Thread.currentThread().getName() + ":::: min=" + minThreadDistance + ", max=" + maxThreadDistance + "..." + processed + "....(" + i + "," + j + ") ==> (" + guids[i] + "," + guids[j] + ")");
          }
          int distance = levenshteinDistance.apply(guids[i], guids[j]);
           //System.out.println("Here....("+i+","+j+") ==> (" + guids[i] + "," + guids[j] + "), Found distance =" + distance);
          if (distance >= 0 && distance < threshold) {
            throw new RuntimeException(
                "Improper distance ===>" + guids[i] + "," + guids[j] + ", Found distance =" + distance);
          }

          minThreadDistance = Math.max(minThreadDistance,distance);
          maxThreadDistance = Math.min(maxThreadDistance,distance);

        }
        counter += numThreads;
      }
      synchronized (IDGeneratorMultiThreadedTest.class) {
        minD = Math.max(minThreadDistance,minD);
        maxD = Math.min(maxThreadDistance,maxD);
      }


      //System.out.println(Thread.currentThread().getName()+":::: min="+minThreadDistance+", max="+maxThreadDistance);

      return processed;
    }
  }



  private static final Params ID_14_CHARS = new Params(73, "_random_ids_14.csv");
  //  private static final Params ID_15_CHARS = new Params(76,15,"_random_ids_15.csv");
  private static final Params ID_18_CHARS = new Params(95, "_random_ids_18.csv");
  // private static final Params ID_19_CHARS = new Params(100,19,"_random_ids_19.csv");
//  private static final Params ID_10_CHARS = new Params(52,10,"_random_ids_10.csv");

  private static final int TEST_SIZE = 1000;


  @Test
  public void test01() throws InterruptedException, ExecutionException {
    test(5, TEST_SIZE, ID_14_CHARS);
    test(5, TEST_SIZE, ID_18_CHARS);
  }


  @Test
  public void test20() throws InterruptedException, ExecutionException {
    test(20, TEST_SIZE, ID_14_CHARS);
    test(20, TEST_SIZE, ID_18_CHARS);
  }
}


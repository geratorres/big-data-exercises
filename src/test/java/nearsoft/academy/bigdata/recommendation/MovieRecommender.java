package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.Lists;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.iterator.FileLineIterator;

import java.io.*;
import java.util.*;

public class MovieRecommender extends FileDataModel {

    int reviewCount;
    BidiMap productIDs;
    Map<String, Integer> userIDs;

    UserBasedRecommender recommender;

    Timer timer;
    Logger logger;
    Thread loggerThread;

    // Amazon movies review dataset format is diferent than FileDataModel's
    // Using this delimiter [:|/] because FileDataModel split first file line with default delimiters [,|\t]
    // to know how many values are per line.
    // [:|/] splits firs line in 3 elements, indicating there are 3 values per list (userID, itemID, Score)

    public MovieRecommender(String path) throws TasteException, IOException {
        super(new File(path), "[:|/]");

        DataModel model = this;
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);

        recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
    }

    public int getTotalReviews() {
        return reviewCount;
    }

    public int getTotalProducts() {
        return productIDs.size();
    }

    public int getTotalUsers() {
        return userIDs.size();
    }

    public List<String> getRecommendationsForUser(String userId) throws TasteException {
        Timer timer = new Timer();
        int howMany = 3;
        List<String> recommendation = new ArrayList<String>(howMany);
        long uID = userIDs.get(userId);

        timer.start("Recommending");
        List<RecommendedItem> recommendations = recommender.recommend(uID, howMany);
        timer.stop();

        for (RecommendedItem r : recommendations) {
            recommendation.add(productIDs.getKey(new Integer(String.valueOf(r.getItemID()))).toString());
        }

        return recommendation;
    }

    @Override
    protected void processFile(FileLineIterator dataOrUpdateFileIterator, FastByIDMap<?> data, FastByIDMap<FastByIDMap<Long>> timestamps, boolean fromPriorData) {
        reviewCount = 0;
        productIDs = new DualHashBidiMap();
        userIDs = new HashMap<String, Integer>();

        timer = new Timer();
        logger = new Logger(5);
        loggerThread = new Thread(logger);
        loggerThread.start();

        timer.start("Parsing File");

        boolean reviewComplete = false;
        long userID = -1l, itemID = -1l;
        float score = -1f;

        String line;
        while (dataOrUpdateFileIterator.hasNext()) {
            line = dataOrUpdateFileIterator.next();

            if (!line.isEmpty() && !((line.indexOf(':') == -1) || reviewComplete)) {
                if (line.charAt(0) == 'p') {
                    itemID = parseId(line.substring(19), productIDs);
                } else if (line.charAt(7) == 'u') {
                    userID = parseId(line.substring(15), userIDs);
                } else if (line.charAt(8) == 'c') {
                    score = Float.parseFloat(line.substring(14));
                    reviewComplete = true;
                }
            } else if (reviewComplete) {
                addReview(userID, itemID, score, data);
                reviewCount++;
                reviewComplete = false;
            }
        }

        logger.stop();
        timer.stop();
        log("Parsed Reviews: " + reviewCount);
    }

    private void addReview(long userID, long itemID, float score, FastByIDMap<?> data) {
        Object maybePrefs = data.get(userID);
        Collection<Preference> prefs = (Collection<Preference>) maybePrefs;

        if (prefs == null) {
            prefs = Lists.newArrayListWithCapacity(2);
            ((FastByIDMap<Collection<Preference>>) data).put(userID, prefs);
        }
        prefs.add(new GenericPreference(userID, itemID, score));
    }

    private Integer parseId(String id, Map<String, Integer> map) {
        Integer value = map.get(id);
        if (value == null) {
            value = map.size();
            map.put(id, value);
        }
        return value;
    }

    private void log(Object... objects) {
        for (Object o : objects) {
            System.out.println(o);
        }
    }

    private class Logger implements Runnable {
        private int interval;
        private boolean go = true;

        public Logger(int interval) {
            this.interval = interval * 1000;
        }

        public void stop() {
            go = false;
        }

        @Override
        public void run() {
            while (go) {
                log("Reviews Parsed: " + reviewCount);
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

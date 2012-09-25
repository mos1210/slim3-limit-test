package slim3.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.slim3.datastore.DaoBase;
import org.slim3.datastore.Datastore;
import org.slim3.datastore.S3QueryResultList;

import com.google.appengine.api.datastore.Key;

import slim3.model.Spot;

public class SpotDao extends DaoBase<Spot> {

    private static final Logger LOG = Logger.getLogger(SpotDao.class
        .getSimpleName());

    public void create(String action) {

        if ("ins".equals(action)) {
            List<Spot> list = new ArrayList<Spot>();
            int cnt = 0;
            for (int i = 0; i < 1001; i++) {
                if (list.size() == 500) {
                    cnt += list.size();
                    Datastore.put(list);
                    list.clear();
                }
                Key id = Datastore.createKey(Spot.class, String.valueOf(i));
                Spot h = new Spot();
                h.setKey(id);
                list.add(h);
            }
            if (list.size() > 0) {
                cnt += list.size();
                Datastore.put(list);
            }
            LOG.info("INSERT Size:" + cnt);
        } else if ("del".equals(action)) {

            int limit = 1000;
            S3QueryResultList<Spot> results =
                Datastore
                    .query(Spot.class)
                    .limit(limit)
                    .prefetchSize(1000)
                    .chunkSize(1000)
                    .asQueryResultList();

            String encodedCursor = results.getEncodedCursor();

            List<Key> keyList = new ArrayList<Key>();
            int delCnt = 0;
            for (Spot entity : results) {
                delCnt += deleteByKey(keyList);
                keyList.add(entity.getKey());
            }
            while (results.hasNext()) {
                encodedCursor = results.getEncodedCursor();
                results =
                    Datastore
                        .query(Spot.class)
                        .encodedStartCursor(encodedCursor)
                        .limit(limit)
                        .prefetchSize(1000)
                        .chunkSize(1000)
                        .asQueryResultList();
                for (Spot result : results) {
                    delCnt += deleteByKey(keyList);
                    keyList.add(result.getKey());
                }
            }
            if (keyList.size() > 0) {
                delCnt += keyList.size();
                Datastore.delete(keyList);
            }
            LOG.info("DELETE COUNT:" + delCnt);
        }
    }

    private int deleteByKey(List<Key> list) {
        if (list.size() == 500) {
            Datastore.delete(list);
            list.clear();
            return 500;
        }
        return 0;
    }

}

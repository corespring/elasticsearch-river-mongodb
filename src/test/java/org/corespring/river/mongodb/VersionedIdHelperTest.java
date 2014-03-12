package org.corespring.river.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Random;

@Test
public class VersionedIdHelperTest {

  public void testObjectIdIsUnchangedByVersionedIdString() {
    ObjectId objectId = new ObjectId();
    Assert.assertEquals(objectId.toString(), VersionedIdHelper.versionedIdString(objectId));
  }

  public void testVersionedIdStringWithBigDecimalReturnsCorrectString() {
    ObjectId objectId = new ObjectId();
    BigDecimal version = new BigDecimal((new Random()).nextInt(9));

    DBObject versionedId = new BasicDBObject();
    versionedId.put(MongoDBRiver.MONGODB_ID_FIELD, objectId);
    versionedId.put(VersionedIdHelper.MONGODB_VERSION_FIELD, version);

    String id = VersionedIdHelper.versionedIdString(versionedId);

    Assert.assertEquals(id, objectId.toString());
  }

  public void testVersionedIdStringWithIntegerReturnsCorrectString() {
    ObjectId objectId = new ObjectId();
    Integer version = (new Random()).nextInt(9);

    DBObject versionedId = new BasicDBObject();
    versionedId.put(MongoDBRiver.MONGODB_ID_FIELD, objectId);
    versionedId.put(VersionedIdHelper.MONGODB_VERSION_FIELD, version);

    String id = VersionedIdHelper.versionedIdString(versionedId);

    Assert.assertEquals(id, objectId.toString());
  }

  public void testGetIdWithVersionedId() {
    ObjectId objectId = new ObjectId();
    String versionedId = objectId + ":" + (new Random()).nextInt(9);

    Assert.assertEquals(VersionedIdHelper.getId(versionedId), objectId);
  }

  public void testGetIdWithUnversionedId() {
    ObjectId objectId = new ObjectId();
    Assert.assertEquals(VersionedIdHelper.getId(objectId), objectId);
  }

  public void testUnversionId() throws InterruptedException {
    ObjectId objectId = new ObjectId();
    BSONObject id = new BasicDBObject();
    id.put(MongoDBRiver.MONGODB_ID_FIELD, objectId);
    id.put(VersionedIdHelper.MONGODB_VERSION_FIELD, 0);

    BSONObject data = new BasicDBObject();
    data.put(MongoDBRiver.MONGODB_ID_FIELD, id);

    BSONObject result = VersionedIdHelper.unversionId(data);
    Object resultId = result.get(MongoDBRiver.MONGODB_ID_FIELD);

    Assert.assertEquals(resultId, objectId);
  }

}

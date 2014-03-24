package org.corespring.river.mongodb;

import com.mongodb.*;
import org.corespring.river.mongodb.StandardsConverter.ContentKeys;
import org.corespring.river.mongodb.StandardsConverter.StandardKeys;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class StandardsConverterTest {

  Mongo mongoClient = mock(Mongo.class);
  DB db = mock(DB.class);
  DBCursor cursor = mock(DBCursor.class);
  DBCollection collection = mock(DBCollection.class);

  String RL45 = "RL.4.5";
  String RL46 = "RL.4.6";
  String W52c = "W.5.2c";
  String L41d = "L.4.1d";

  DBObject RL45Standard = standard(RL45,
    "Reading: Literature",
    "Determine the meaning of words and phrases as they are used in a text, includin",
    "Craft and Structure", "extra data");
  DBObject RL46Standard = standard(RL46,
    "Reading: Literature",
    "Compare and contrast the point of view from which different stories are narrate",
    "Craft and Structure", "extraneous data");
  DBObject W52cStandard = standard(W52c, "Writing",
    "Link ideas within and across categories of information using words, phrases, and clause",
    "Text Types and Purposes", "this should be missing");
  DBObject L41dStandard = standard(L41d, "Language",
    "Order adjectives within sentences according to conventional patterns (e.g., <em>a smal",
    "Conventions of Standard English", "this isn't necessary");

  Map<String, DBObject> fakeStandardsMap = new HashMap<String, DBObject>() {
    {
      put(RL45, RL45Standard);
      put(RL46, RL46Standard);
      put(W52c, W52cStandard);
      put(L41d, L41dStandard);
    }
  };

  private DBObject standard(String dotNotation, String category, String standard, String subCategory, String extra) {
    Map<String, String> map = new HashMap<String, String>();
    map.put(StandardKeys.DOT_NOTATION, dotNotation);
    map.put(StandardKeys.CATEGORY, category);
    map.put(StandardKeys.STANDARD, standard);
    map.put(StandardKeys.SUBCATEGORY, subCategory);
    map.put("extra", extra);
    DBObject object = new BasicDBObject();
    object.putAll(map);
    return object;
  }

  Collection<DBObject> fakeStandards = new ArrayList<DBObject>() { {
    add(RL45Standard); add(RL46Standard); add(W52cStandard); add(L41dStandard);
  } };
  MongoDBRiverDefinition definition = mock(MongoDBRiverDefinition.class);
  StandardsConverter standardsConverter;

  {
    when(mongoClient.getDB(anyString())).thenReturn(db);
    when(db.getCollection(anyString())).thenReturn(collection);
    when(definition.getMongoDb()).thenReturn("db");
    when(definition.getMongoCollection()).thenReturn("collection");
    when(collection.find()).thenReturn(cursor);
    when(cursor.iterator()).thenReturn(fakeStandards.iterator());

    standardsConverter = new StandardsConverter(mongoClient, definition);
  }

  @Test
  public void testAddStandardData() throws InterruptedException {
    DBObject objectWithStandardsDotNotation = new BasicDBObject();
    BasicDBList standardsNotation = new BasicDBList();
    standardsNotation.add(RL45);
    standardsNotation.add(RL46);
    standardsNotation.add(W52c);
    standardsNotation.add(L41d);
    objectWithStandardsDotNotation.put(StandardsConverter.ContentKeys.STANDARDS, standardsNotation);

    DBObject objectWithStandards = standardsConverter.addStandardData(objectWithStandardsDotNotation);

    BasicDBList list = (BasicDBList) objectWithStandards.get(ContentKeys.STANDARDS);

    for (Iterator<Object> iterator = list.iterator(); iterator.hasNext();) {
      DBObject standardObject = (DBObject) iterator.next();
      String dotNotation = (String) standardObject.get(StandardKeys.DOT_NOTATION);
      DBObject dbStandard = fakeStandardsMap.get(dotNotation);

      // Verify the new fields have been added
      assertEquals(standardObject.get(StandardKeys.CATEGORY), dbStandard.get(StandardKeys.CATEGORY));
      assertEquals(standardObject.get(StandardKeys.STANDARD), dbStandard.get(StandardKeys.STANDARD));
      assertEquals(standardObject.get(StandardKeys.SUBCATEGORY), dbStandard.get(StandardKeys.SUBCATEGORY));

      // Verify the extraneous data has not been added
      assertNull(standardObject.get("extra"));
    }

  }

}

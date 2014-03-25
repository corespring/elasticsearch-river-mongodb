package org.corespring.river.mongodb;

import com.mongodb.*;
import org.corespring.river.mongodb.StandardsConverter.ContentKeys;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class StandardsConverterTest {

  String RL45 = "RL.4.5";
  String RL46 = "RL.4.6";
  String W52c = "W.5.2c";
  String L41d = "L.4.1d";

  Map RL45Standard = standard(RL45,
    "Reading: Literature",
    "Determine the meaning of words and phrases as they are used in a text, includin",
    "Craft and Structure", "extra data");
  Map RL46Standard = standard(RL46,
    "Reading: Literature",
    "Compare and contrast the point of view from which different stories are narrate",
    "Craft and Structure", "extraneous data");
  Map W52cStandard = standard(W52c, "Writing",
    "Link ideas within and across categories of information using words, phrases, and clause",
    "Text Types and Purposes", "this should be missing");
  Map L41dStandard = standard(L41d, "Language",
    "Order adjectives within sentences according to conventional patterns (e.g., <em>a smal",
    "Conventions of Standard English", "this isn't necessary");

  private Map standard(String dotNotation, String category, String standard, String subCategory, String extra) {
    Map<String, String> map = new HashMap<String, String>();
    map.put(Standard.Keys.DOT_NOTATION, dotNotation);
    map.put(Standard.Keys.CATEGORY, category);
    map.put(Standard.Keys.STANDARD, standard);
    map.put(Standard.Keys.SUBCATEGORY, subCategory);
    map.put("extra", extra);
    return map;
  }

  Collection<Map> fakeStandards = new ArrayList<Map>() { {
    add(RL45Standard); add(RL46Standard); add(W52cStandard); add(L41dStandard);
  } };

  StandardsConverter standardsConverter;

  {
    StandardsDAO standardsDAO = mock(StandardsDAO.class);
    when(standardsDAO.getStandards()).thenReturn(fakeStandards);
    standardsConverter = new StandardsConverter(standardsDAO);
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
      String dotNotation = (String) standardObject.get(Standard.Keys.DOT_NOTATION);
      Map dbStandard = getStandardByDotNotation(dotNotation, fakeStandards);

      // Verify the new fields have been added
      assertEquals(standardObject.get(Standard.Keys.CATEGORY), dbStandard.get(Standard.Keys.CATEGORY));
      assertEquals(standardObject.get(Standard.Keys.STANDARD), dbStandard.get(Standard.Keys.STANDARD));
      assertEquals(standardObject.get(Standard.Keys.SUBCATEGORY), dbStandard.get(Standard.Keys.SUBCATEGORY));

      // Verify the extraneous data has not been added
      assertNull(standardObject.get("extra"));
    }

  }

  private Map getStandardByDotNotation(String dotNotation, Collection<Map> standards) {
    for (Map standard : standards) {
      if (standard.containsKey(Standard.Keys.DOT_NOTATION) &&
        standard.get(Standard.Keys.DOT_NOTATION).equals(dotNotation)) {
        return standard;
      }
    }
    return null;
  }

}

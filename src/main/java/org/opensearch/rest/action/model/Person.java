package org.opensearch.rest.action.model;

import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Locale;

public class Person implements ToXContent {

    public static final String NAME_FIELD = "name";
    public static final String AGE_FIELD = "age";

    private String id;
    private String name;
    private int age;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field(NAME_FIELD, getName())
                .field(AGE_FIELD, getAge())
                .endObject();
        return builder;
    }

    public static Person parse(XContentParser parser, String personId) throws IOException {
        Person person = new Person();
        person.setId(personId);

        XContentParserUtils.ensureExpectedToken(
                XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case NAME_FIELD:
                    person.setName(parser.textOrNull());
                    break;
                case AGE_FIELD:
                    person.setAge(parser.intValue());
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT,
                                    "Unknown person field %s", fieldName));
            }
        }

        return person;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

package org.example.avro;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class AvroTest {

    @Test
    public void test1() throws IOException {
        final Schema.Parser parser = new Schema.Parser();
        String text = "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
                "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
                " ]\n" +
                "}";
        final Schema schema = parser.parse(text);

        System.out.println(schema.toString());

        final SpecificDatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(schema);
        final DataFileWriter<User> userDataFileWriter = new DataFileWriter<>(userDatumWriter);

        User user1 = new User("zhangsan",1,"red");
        userDataFileWriter.create(schema,new File("/tmp/user.avro"));
        userDataFileWriter.append(user1);
        userDataFileWriter.flush();
        userDataFileWriter.close();


    }

    @Test
    public void read() throws IOException {
        final File file = new File("file:///tmp/user.avro");
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

    @Test
    public void test2(){
        String[] words = {"aa","aa"};
        final List<Integer> ans = new ArrayList<>();
        final HashMap<HashSet<Character>, Integer> map = new HashMap<>();
        for (String word : words) {
            final HashSet<Character> characters = new HashSet<>();
            for (char c : word.toCharArray()) {
                characters.add(c);
            }
            map.put(characters,map.getOrDefault(characters,0)+1);
        }
        for (HashSet<Character> characters : map.keySet()) {
            final Integer value = map.get(characters);
            System.out.println(value);
        }
    }
}

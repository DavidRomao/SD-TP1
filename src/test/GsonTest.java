package test;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

public class GsonTest {
    public static void main(String[] args) {
        Gson gson = new Gson();
        List<String>  list =  new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add("helloooo  " + i);
        }
        String s = gson.toJson(list);
//        List<String> a = new LinkedList<>();
        List<String> list1 = gson.fromJson(s, List.class);
        System.out.println(s);
//        for (String s1 : list1) {
//            System.out.println(s1);
//        }
    }
}


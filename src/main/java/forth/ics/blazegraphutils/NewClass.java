/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forth.ics.blazegraphutils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 *
 * @author rousakis
 */
public class NewClass {

    public static void main(String[] args) throws Exception {

//        for (File file : new File("input/Fishbase").listFiles()) {
//            fixFile(file);
//        }
        fixFile(new File("input/lifewatch5a.nt"));

    }

    public static void fixFile(File file) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file + "_fixed"), "UTF-8"));
        String line;
        int cnt = 1;
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null) {
//            String[] tokens = line.split("\\s+");

            String[] tokens = line.split("> ");

            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
//                token = token.replaceAll(">", "").replace(" .", "").replace("<", "");
//                if (token.contains("http://www.fishbase.org/entity#") && token.contains(" ")) {
//                    token = token.replaceAll(" ", "_");
//                    System.out.println(cnt + "\t" + token  );
//                }

//                System.out.print(token);
                token = token.trim();
                if (token.contains("http")) {
                    token += ">";
                }
                if (!token.contains("http") && !token.equals(".")) {
                    token = "\"" + token.substring(0, token.length() - 2) + "\"";
                }
                if (token.equals(".")) {
                    continue;
                }

//                if ((token.contains("http://") || token.contains("<http://"))) {
//                    if (!token.endsWith(">")) {
//                        token = token + ">";
//                    }
//                }
                sb.append("").append(token).append(" ");

            }
            sb.append(".\n");
            bw.append(sb);
            sb.setLength(0);
            cnt++;
        }
        br.close();
        bw.close();
        System.out.println("-DONE-");
    }

}

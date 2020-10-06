package org.apache.beam.example;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

public class TestTask {

  private static final String GMAIL_DOMAIN = "@gmail.com";
  private static final String INPUT_PATH = "input/";
  private static final String OUTPUTS_PATH = "output/";
  private static final String ERROR_PATH = /*OUTPUTS_PATH +*/ "error/";
  private static final String RECORD_CONTAINS_AGE_LESS_20 = "a record contains age less then 20";
  private static final String RECORD_CONTAINS_NOT_GMAIL_MAIL = "a record contains not Gmail email";
  private static final String TRANSFORM = "transform";
  private static final String SAVE = "save";
  private static final String READ = "read";
  private static final String USERS_WITH_NOT_GMAIL_MAIL = "usersWithNotGmailMail";
  private static final String USERS_YOUNGER_THAN_20 = "usersYoungerThan20";
  private static final String AVRO = ".avro";
  private static final String USERS_INPUT = "users";
  private static final String AGE_STRING = "ageString";
  private static final String EMAIL = "email";
  private static final String AGE = "age";
  private static final String USER_NAME = "userName";
  private static final String UNKNOWN = "unknown";
  private static final TupleTag<User> passed = new TupleTag<>() {};
  private static final TupleTag<User> youngerThan20 = new TupleTag<>() {};
  private static final TupleTag<User> mailIsNotGmail = new TupleTag<>() {};

  public static void main(String[] args) {
    File usersInput = new File(INPUT_PATH + USERS_INPUT + AVRO);

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    options.setStableUniqueNames(CheckEnabled.OFF);
    Pipeline pipeline = Pipeline.create(options);

    PCollectionView<Map<Integer, String>> ageInWords =
        pipeline
            .apply(
                READ,
                AvroIO.parseGenericRecords(new ParseAgeInWords())
                    .from(INPUT_PATH + AGE_STRING + AVRO))
            .apply(View.asMap());

    PCollection<User> users =
        pipeline.apply(
            READ, AvroIO.parseGenericRecords(new ParseUser()).from(usersInput.getAbsolutePath()));

    PCollectionTuple allUsers =
        users.apply(
            ParDo.of(new ProcessUser())
                .withOutputTags(passed, TupleTagList.of(List.of(youngerThan20, mailIsNotGmail))));

    allUsers
        .get(passed)
        .apply(ParDo.of(new EnhancePassedUser(ageInWords)).withSideInputs(ageInWords))
        .apply(
            SAVE,
            AvroIO.write(PassedUser.class).withoutSharding().to(OUTPUTS_PATH + USERS_INPUT + AVRO));

    allUsers
        .get(youngerThan20)
        .apply(
            TRANSFORM,
            MapElements.into(TypeDescriptor.of(ErrorUser.class))
                .via(input -> new ErrorUser(input, RECORD_CONTAINS_AGE_LESS_20)))
        .apply(
            SAVE,
            AvroIO.write(ErrorUser.class)
                .withoutSharding()
                .to(ERROR_PATH + USERS_YOUNGER_THAN_20 + AVRO));

    allUsers
        .get(mailIsNotGmail)
        .apply(
            TRANSFORM,
            MapElements.into(TypeDescriptor.of(ErrorUser.class))
                .via(input -> new ErrorUser(input, RECORD_CONTAINS_NOT_GMAIL_MAIL)))
        .apply(
            SAVE,
            AvroIO.write(ErrorUser.class)
                .withoutSharding()
                .to(ERROR_PATH + USERS_WITH_NOT_GMAIL_MAIL + AVRO));

    pipeline.run();
  }

  @DefaultCoder(AvroCoder.class)
  static class ErrorUser {
    User user;
    String error;

    public ErrorUser() {}

    public ErrorUser(User user, String error) {
      this.user = user;
      this.error = error;
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class PassedUser {
    Integer age;
    String email;
    String userName;
    String shortUserName;
    String ageString;

    public PassedUser() {}

    public PassedUser(User user, String shortUserName, String ageString) {
      this.age = user.age;
      this.email = user.email;
      this.userName = user.userName;
      this.shortUserName = shortUserName;
      this.ageString = ageString;
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class User {
    String email;
    Integer age;
    String userName;

    public User() {}

    public User(String email, Integer age, String userName) {
      this.email = email;
      this.age = age;
      this.userName = userName;
    }
  }

  private static class ProcessUser extends DoFn<User, User> {
    @ProcessElement
    public void processElement(ProcessContext processContext) {
      User user = processContext.element();
      if (user.age < 20) {
        processContext.output(youngerThan20, processContext.element());
      }
      if (!user.email.endsWith(GMAIL_DOMAIN)) {
        processContext.output(mailIsNotGmail, processContext.element());
      }
      if (user.age >= 20 && user.email.endsWith(GMAIL_DOMAIN)) {
        processContext.output(passed, processContext.element());
      }
    }
  }

  private static class EnhancePassedUser extends DoFn<User, PassedUser> {
    private final PCollectionView<Map<Integer, String>> ageInWords;

    public EnhancePassedUser(PCollectionView<Map<Integer, String>> ageInWords) {
      this.ageInWords = ageInWords;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      Map<Integer, String> sideInputList = processContext.sideInput(ageInWords);
      var shortUserName =
          processContext
              .element()
              .email
              .substring(0, processContext.element().email.lastIndexOf(GMAIL_DOMAIN));
      var ageString = sideInputList.getOrDefault(processContext.element().age, UNKNOWN);
      processContext.output(new PassedUser(processContext.element(), shortUserName, ageString));
    }
  }

  private static class ParseUser implements SerializableFunction<GenericRecord, User> {
    @Override
    public User apply(GenericRecord input) {
      return new User(
          input.get(EMAIL).toString(), (Integer) input.get(AGE), input.get(USER_NAME).toString());
    }
  }

  private static class ParseAgeInWords
      implements SerializableFunction<GenericRecord, KV<Integer, String>> {
    @Override
    public KV<Integer, String> apply(GenericRecord input) {
      return KV.of((Integer) input.get(AGE), input.get(AGE_STRING).toString());
    }
  }
}

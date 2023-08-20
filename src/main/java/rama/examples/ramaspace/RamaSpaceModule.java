package rama.examples.ramaspace;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TaskUniqueIdPState;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.module.*;
import rama.examples.ramaspace.data.*;
import com.rpl.rama.ops.Ops;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

public class RamaSpaceModule implements RamaModule {

  private static void declareUsersTopology(Topologies topologies) {
    StreamTopology users = topologies.stream("users");
    users.pstate(
      "$$profiles",
      PState.mapSchema(
        String.class,
        PState.fixedKeysSchema(
          "displayName", String.class,
          "email", String.class,
          "profilePic", String.class,
          "bio", String.class,
          "location", String.class,
          "pwdHash", Integer.class,
          "joinedAtMillis", Long.class,
          "registrationUUID", String.class)));

    users.source("*userRegistrationsDepot").out("*registration")
         .macro(extractJavaFields("*registration", "*userId", "*email", "*displayName", "*pwdHash", "*registrationUUID"))
         .each(System::currentTimeMillis).out("*joinedAtMillis")
         .localTransform("$$profiles",
           Path.key("*userId")
               .filterPred(Ops.IS_NULL)
               .multiPath(Path.key("email").termVal("*email"),
                          Path.key("displayName").termVal("*displayName"),
                          Path.key("pwdHash").termVal("*pwdHash"),
                          Path.key("joinedAtMillis").termVal("*joinedAtMillis"),
                          Path.key("registrationUUID").termVal("*registrationUUID")));

    users.source("*profileEditsDepot").out("*edit")
         .macro(extractJavaFields("*edit", "*userId", "*field", "*value"))
         .localTransform("$$profiles", Path.key("*userId", "*field").termVal("*value"));
  }

  private static void declareFriendsTopology(Topologies topologies) {
    StreamTopology friends = topologies.stream("friends");
    friends.pstate(
      "$$outgoingFriendRequests",
      PState.mapSchema(
        String.class,
        PState.setSchema(String.class).subindexed()));
    friends.pstate(
      "$$incomingFriendRequests",
      PState.mapSchema(
        String.class,
        PState.setSchema(String.class).subindexed()));
    friends.pstate(
      "$$friends",
      PState.mapSchema(
        String.class,
        PState.setSchema(String.class).subindexed()));

    friends.source("*friendRequestsDepot").out("*request")
           .macro(extractJavaFields("*request", "*userId", "*toUserId"))
           .subSource("*request",
             SubSource.create(FriendRequest.class)
                      .compoundAgg("$$outgoingFriendRequests", CompoundAgg.map("*userId", Agg.set("*toUserId")))
                      .hashPartition("*toUserId")
                      .compoundAgg("$$incomingFriendRequests", CompoundAgg.map("*toUserId", Agg.set("*userId"))),
             SubSource.create(CancelFriendRequest.class)
                      .compoundAgg("$$outgoingFriendRequests", CompoundAgg.map("*userId", Agg.setRemove("*toUserId")))
                      .hashPartition("*toUserId")
                      .compoundAgg("$$incomingFriendRequests", CompoundAgg.map("*toUserId", Agg.setRemove("*userId"))));

    friends.source("*friendshipChangesDepot").out("*change")
           .macro(extractJavaFields("*change", "*userId1", "*userId2"))
           .anchor("start")
           .compoundAgg("$$incomingFriendRequests", CompoundAgg.map("*userId1", Agg.setRemove("*userId2")))
           .compoundAgg("$$outgoingFriendRequests", CompoundAgg.map("*userId1", Agg.setRemove("*userId2")))
           .hashPartition("*userId2")
           .compoundAgg("$$incomingFriendRequests", CompoundAgg.map("*userId2", Agg.setRemove("*userId1")))
           .compoundAgg("$$outgoingFriendRequests", CompoundAgg.map("*userId2", Agg.setRemove("*userId1")))
           .hook("start")
           .subSource("*change",
             SubSource.create(FriendshipAdd.class)
                      .compoundAgg("$$friends", CompoundAgg.map("*userId1", Agg.set("*userId2")))
                      .hashPartition("*userId2")
                      .compoundAgg("$$friends", CompoundAgg.map("*userId2", Agg.set("*userId1"))),
             SubSource.create(FriendshipRemove.class)
                      .compoundAgg("$$friends", CompoundAgg.map("*userId1", Agg.setRemove("*userId2")))
                      .hashPartition("*userId2")
                      .compoundAgg("$$friends", CompoundAgg.map("*userId2", Agg.setRemove("*userId1"))));
  }

  private static void declarePostsTopology(Topologies topologies) {
    MicrobatchTopology posts = topologies.microbatch("posts");
    posts.pstate(
      "$$posts",
      PState.mapSchema(
        String.class,
        PState.mapSchema(Long.class, Post.class).subindexed()));

    TaskUniqueIdPState id = new TaskUniqueIdPState("$$postId").descending();
    id.declarePState(posts);

    posts.source("*postsDepot").out("*microbatch")
         .explodeMicrobatch("*microbatch").out("*post")
         .macro(extractJavaFields("*post", "*toUserId"))
         .macro(id.genId("*id"))
         .localTransform("$$posts", Path.key("*toUserId", "*id").termVal("*post"));
  }

  private static void declareProfileViewsTopology(Topologies topologies) {
    MicrobatchTopology profileViews = topologies.microbatch("profileViews");
    profileViews.pstate(
      "$$profileViews",
      PState.mapSchema(
        String.class,
        PState.mapSchema(Long.class, Long.class).subindexed()));

    profileViews.source("*profileViewsDepot").out("*microbatch")
                .explodeMicrobatch("*microbatch").out("*profileView")
                .macro(extractJavaFields("*profileView", "*toUserId", "*timestamp"))
                .each((Long timestamp) -> timestamp / (1000 * 60 * 60), "*timestamp").out("*bucket")
                .compoundAgg("$$profileViews",
                  CompoundAgg.map(
                    "*toUserId",
                    CompoundAgg.map(
                      "*bucket",
                      Agg.count())));
  }

  public static class UserIdExtract extends TopologyUtils.ExtractJavaField {
    public UserIdExtract() {
      super("userId");
    }
  }

  public static class ToUserIdExtract extends TopologyUtils.ExtractJavaField {
    public ToUserIdExtract() {
      super("toUserId");
    }
  }

  public static class UserId1Extract extends TopologyUtils.ExtractJavaField {
    public UserId1Extract() {
      super("userId1");
    }
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*userRegistrationsDepot", Depot.hashBy(UserIdExtract.class));
    setup.declareDepot("*profileEditsDepot", Depot.hashBy(UserIdExtract.class));
    setup.declareDepot("*profileViewsDepot", Depot.hashBy(ToUserIdExtract.class));
    setup.declareDepot("*friendRequestsDepot", Depot.hashBy(UserIdExtract.class));
    setup.declareDepot("*friendshipChangesDepot", Depot.hashBy(UserId1Extract.class));
    setup.declareDepot("*postsDepot", Depot.hashBy(ToUserIdExtract.class));

    declareUsersTopology(topologies);
    declareFriendsTopology(topologies);
    declarePostsTopology(topologies);
    declareProfileViewsTopology(topologies);

    topologies.query("resolvePosts", "*forUserId", "*startPostId").out("*resultMap")
              .hashPartition("*forUserId")
              .localSelect("$$posts", Path.key("*forUserId").sortedMapRangeFrom("*startPostId", 20)).out("*submap")
              .each(Ops.EXPLODE_MAP, "*submap").out("*i", "*post")
              .macro(extractJavaFields("*post", "*userId", "*content"))
              .hashPartition("*userId")
              .localSelect("$$profiles", Path.key("*userId", "displayName")).out("*displayName")
              .localSelect("$$profiles", Path.key("*userId", "profilePic")).out("*profilePic")
              .each(ResolvedPost::new, "*userId", "*content", "*displayName", "*profilePic").out("*resolvedPost")
              .originPartition()
              .compoundAgg(CompoundAgg.map("*i", Agg.last("*resolvedPost"))).out("*resultMap");
  }
}

package rama.examples.ramaspace;

import com.rpl.rama.*;
import com.rpl.rama.cluster.*;
import rama.examples.ramaspace.data.*;
import com.rpl.rama.ops.Ops;

import java.util.*;

public class RamaSpaceClient {
  private Depot _userRegistrationsDepot;
  private Depot _profileEditsDepot;
  private Depot _profileViewsDepot;
  private Depot _friendRequestsDepot;
  private Depot _friendshipChangesDepot;
  private Depot _postsDepot;

  private PState _profiles;
  private PState _outgoingFriendRequests;
  private PState _incomingFriendRequests;
  private PState _friends;
  private PState _posts;
  private PState _profileViews;

  private QueryTopologyClient<Map<Long, ResolvedPost>> _resolvePosts;

  public RamaSpaceClient(ClusterManagerBase cluster) {
    String moduleName = RamaSpaceModule.class.getName();
    _userRegistrationsDepot = cluster.clusterDepot(moduleName, "*userRegistrationsDepot");
    _profileEditsDepot = cluster.clusterDepot(moduleName, "*profileEditsDepot");
    _profileViewsDepot = cluster.clusterDepot(moduleName, "*profileViewsDepot");
    _friendRequestsDepot = cluster.clusterDepot(moduleName, "*friendRequestsDepot");
    _friendshipChangesDepot = cluster.clusterDepot(moduleName, "*friendshipChangesDepot");
    _postsDepot = cluster.clusterDepot(moduleName, "*postsDepot");

    _profiles = cluster.clusterPState(moduleName, "$$profiles");
    _outgoingFriendRequests = cluster.clusterPState(moduleName, "$$outgoingFriendRequests");
    _incomingFriendRequests = cluster.clusterPState(moduleName, "$$incomingFriendRequests");
    _friends = cluster.clusterPState(moduleName, "$$friends");
    _posts = cluster.clusterPState(moduleName, "$$posts");
    _profileViews = cluster.clusterPState(moduleName, "$$profileViews");

    _resolvePosts = cluster.clusterQuery(moduleName, "resolvePosts");
  }


  public boolean appendUserRegistration(String userId, String email, String displayName, int pwdHash) {
    String registrationUUID = UUID.randomUUID().toString();
    _userRegistrationsDepot.append(new UserRegistration(userId, email, displayName, pwdHash, registrationUUID));
    String storedUUID = _profiles.selectOne(Path.key(userId, "registrationUUID"));
    return registrationUUID.equals(storedUUID);
  }

  private void appendProfileEdit(String userId, String field, Object value) {
    _profileEditsDepot.append(new ProfileEdit(userId, field, value));
  }

  public void appendBioEdit(String userId, String bio) {
    appendProfileEdit(userId, "bio", bio);
  }

  public void appendDisplayNameEdit(String userId, String displayName) {
    appendProfileEdit(userId, "displayName", displayName);
  }

  public void appendEmailEdit(String userId, String email) {
    appendProfileEdit(userId, "email", email);
  }

  public void appendLocationEdit(String userId, String location) {
    appendProfileEdit(userId, "location", location);
  }

  public void appendProfilePicEdit(String userId, String profilePic) {
    appendProfileEdit(userId, "profilePic", profilePic);
  }

  public void appendPwdHashEdit(String userId, int pwdHash) {
    appendProfileEdit(userId, "pwdHash", pwdHash);
  }

  public void appendProfileView(String userId, String toUserId) {
    _profileViewsDepot.append(new ProfileView(userId, toUserId, System.currentTimeMillis()));
  }

  public void appendFriendRequest(String userId, String toUserId) {
    _friendRequestsDepot.append(new FriendRequest(userId, toUserId));
  }

  public void appendCancelFriendRequest(String userId, String toUserId) {
    _friendRequestsDepot.append(new CancelFriendRequest(userId, toUserId));
  }

  public void appendFriendshipAdd(String userId, String toUserId) {
    _friendshipChangesDepot.append(new FriendshipAdd(userId, toUserId));
  }

  public void appendFriendshipRemove(String userId, String toUserId) {
    _friendshipChangesDepot.append(new FriendshipRemove(userId, toUserId));
  }

  public void appendPost(String userId, String toUserId, String content) {
    _postsDepot.append(new Post(userId, toUserId, content));
  }

  public Integer getPwdHash(String userId) {
    return _profiles.selectOne(Path.key(userId, "pwdHash"));
  }

  public Profile getProfile(String userId) {
    Map profile = _profiles.selectOne(
                    Path.key(userId)
                        .subMap("displayName",
                                "location",
                                "bio",
                                "email",
                                "profilePic",
                                "joinedAtMillis"));
    if(profile.isEmpty()) return null;
    else return new Profile((String) profile.get("email"),
                            (String) profile.get("displayName"),
                            (String) profile.get("bio"),
                            (String) profile.get("location"),
                            (String) profile.get("profilePic"),
                            (long) profile.get("joinedAtMillis"));
  }

  public long getFriendsCount(String userId) {
    return _friends.selectOne(Path.key(userId).view(Ops.SIZE));
  }

  public boolean isFriends(String userId1, String userId2) {
    return !_friends.select(Path.key(userId1).setElem(userId2)).isEmpty();
  }

  public long getPostsCount(String userId) {
    return _posts.selectOne(Path.key(userId).view(Ops.SIZE));
  }

  public Set<String> getOutgoingFriendRequests(String userId, String start) {
    return _outgoingFriendRequests.selectOne(Path.key(userId).sortedSetRangeFrom(start, SortedRangeFromOptions.maxAmt(20).excludeStart()));
  }

  public Set<String> getIncomingFriendRequests(String userId, String start) {
    return _incomingFriendRequests.selectOne(Path.key(userId).sortedSetRangeFrom(start, SortedRangeFromOptions.maxAmt(20).excludeStart()));
  }

  public Set<String> getFriendsPage(String userId, String start) {
    return _friends.selectOne(Path.key(userId).sortedSetRangeFrom(start, SortedRangeFromOptions.maxAmt(20).excludeStart()));
  }

  public long getNumProfileViews(String userId, long startHourBucket, long endHourBucket) {
    return _profileViews.selectOne(
             Path.key(userId)
                 .sortedMapRange(startHourBucket, endHourBucket)
                 .subselect(Path.mapVals())
                 .view(Ops.SUM));
  }

  public TreeMap<Long, ResolvedPost> resolvePosts(String userId, long index) {
    return new TreeMap(_resolvePosts.invoke(userId, index));
  }
}

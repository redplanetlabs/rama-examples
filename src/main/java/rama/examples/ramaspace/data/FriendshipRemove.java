package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class FriendshipRemove implements RamaSerializable {
  public String userId1;
  public String userId2;

  public FriendshipRemove(String userId1, String userId2) {
    this.userId1 = userId1;
    this.userId2 = userId2;
  }
}
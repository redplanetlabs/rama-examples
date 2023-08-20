package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class FriendshipAdd implements RamaSerializable {
  public String userId1;
  public String userId2;

  public FriendshipAdd(String userId1, String userId2) {
    this.userId1 = userId1;
    this.userId2 = userId2;
  }
}
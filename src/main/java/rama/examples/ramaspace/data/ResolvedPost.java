package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class ResolvedPost implements RamaSerializable {
  public String userId;
  public String content;
  public String displayName;
  public String profilePic;

  public ResolvedPost(String userId, String content, String displayName, String profilePic) {
    this.userId = userId;
    this.content = content;
    this.displayName = displayName;
    this.profilePic = profilePic;
  }
}

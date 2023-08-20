package rama.examples.ramaspace;

import com.rpl.rama.test.*;
import org.junit.Test;
import rama.examples.ramaspace.data.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


public class RamaSpaceModuleTest {

  @Test
  public void basicTest() throws Exception {
    try(InProcessCluster ipc = InProcessCluster.create()) {
      RamaSpaceModule ramaspace = new RamaSpaceModule();
      String moduleName = ramaspace.getClass().getName();
      ipc.launchModule(ramaspace, new LaunchConfig(4, 4));
      RamaSpaceClient client = new RamaSpaceClient(ipc);

      assertTrue(client.appendUserRegistration("alice", "alice@gmail.com", "Alice Alice", 1));
      assertFalse(client.appendUserRegistration("alice", "alice2@gmail.com", "Alice2", 2));
      assertEquals(1, client.getPwdHash("alice"));
      client.appendBioEdit("alice", "in wonderland");
      Profile profile = client.getProfile("alice");
      assertEquals("Alice Alice", profile.displayName);
      assertEquals("alice@gmail.com", profile.email);
      assertEquals("in wonderland", profile.bio);
      assertTrue(profile.joinedAtMillis > 0);
      assertNull(profile.location);

      assertTrue(client.appendUserRegistration("bob", "bob@gmail.com", "Bobby", 2));
      assertTrue(client.appendUserRegistration("charlie", "charlie@gmail.com", "Charles", 2));

      for(int i=0; i<8; i++) {
        client.appendPost("alice", "alice", "x" + i);
        client.appendPost("charlie", "alice", "y" + i);
        client.appendPost("bob", "alice", "z" + i);
      }

      ipc.waitForMicrobatchProcessedCount(moduleName, "posts", 24);

      TreeMap<Long, ResolvedPost> page1 = client.resolvePosts("alice", 0);
      assertEquals(20, page1.size());
      TreeMap<Long, ResolvedPost> page2  = client.resolvePosts("alice", page1.lastKey() + 1);
      assertEquals(4, page2.size());
    }
  }
}

import java.util.concurrent.{ExecutionException, Future}

package object experiments {

  implicit class RichFuture [A] (f: Future [A]) {

    def safeGet: A =
      try {
        f.get
      } catch {
        case t: ExecutionException =>
          throw t.getCause
      }}}

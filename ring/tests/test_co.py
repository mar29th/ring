from ring import co
from ring.tests.utils import coroutine_test, AsyncTestCase


class TestCoroutine(AsyncTestCase):

    @co.coroutine
    def _coroutine_a(self):
        raise co.Return('a')

    @co.coroutine
    def _coroutine_b(self):
        raise co.Return('b')

    @coroutine_test
    def test_simple_coroutine(self):
        res = yield self._coroutine_a()
        self.assertEqual(res, 'a')
        res = yield self._coroutine_b()
        self.assertEqual(res, 'b')

    @co.coroutine
    def _test_raise_exception(self):
        raise RuntimeError

    @coroutine_test
    def test_raise_exception(self):
        try:
            yield self._test_raise_exception()
            self.fail('Should raise exception')
        except Exception as e:
            self.assertIsInstance(e, RuntimeError)

    @co.coroutine
    def _test_moment(self):
        obj = []
        self._io_loop.next_tick(lambda: obj.append(None))
        yield co.moment()
        self.assertEqual(len(obj), 1)

    @coroutine_test
    def test_moment(self):
        yield self._test_moment()

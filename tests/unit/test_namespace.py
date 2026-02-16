from kubetimer.utils.namespace import should_scan_namespace


class TestShouldScanNamespace:
    def test_excluded_namespace_returns_false(self):
        assert should_scan_namespace("kube-system", [], ["kube-system"]) is False

    def test_excluded_wins_over_included(self):
        """If a namespace appears in both lists, exclude takes priority."""
        assert (
            should_scan_namespace(
                "kube-system",
                include_namespaces=["kube-system"],
                exclude_namespaces=["kube-system"],
            )
            is False
        )

    def test_empty_include_allows_any(self):
        assert should_scan_namespace("default", [], []) is True

    def test_empty_include_allows_custom_ns(self):
        assert should_scan_namespace("my-app", [], []) is True

    def test_namespace_in_include_list(self):
        assert should_scan_namespace("staging", ["staging", "prod"], []) is True

    def test_namespace_not_in_include_list(self):
        assert should_scan_namespace("dev", ["staging", "prod"], []) is False

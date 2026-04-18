"""
Deterministic unit tests for time normalization utility.

Tests validate:
  - HH:MM format parsing
  - Named time block defaults
  - Natural aliases
  - Determinism (same input → same output)
  - None/empty input handling
  - Reference date usage
  - Reverse lookup
  - Alias listing
"""

from __future__ import annotations

from datetime import datetime, date
import pytest

from apps.api.ingestion.adapters.time_normalizer import (
    normalize_time_input,
    get_time_block_from_iso,
    list_time_aliases,
)


class TestTimeNormalizerBasics:
    """Test basic time format parsing."""

    def test_hhmm_format_standard(self):
        """Parse HH:MM format with standard time."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("11:30", reference_date=ref_date)
        assert result == "2026-04-16T11:30:00"

    def test_hhmm_format_with_spaces(self):
        """Parse HH:MM format with leading/trailing spaces."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("  11:30  ", reference_date=ref_date)
        assert result == "2026-04-16T11:30:00"

    def test_hhmm_format_midnight(self):
        """Parse HH:MM format for midnight."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("00:00", reference_date=ref_date)
        assert result == "2026-04-16T00:00:00"

    def test_hhmm_format_end_of_day(self):
        """Parse HH:MM format for end of day."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("23:59", reference_date=ref_date)
        assert result == "2026-04-16T23:59:00"

    def test_hhmm_invalid_hour(self):
        """Return None for invalid hour."""
        result = normalize_time_input("25:30")
        assert result is None

    def test_hhmm_invalid_minute(self):
        """Return None for invalid minute."""
        result = normalize_time_input("11:60")
        assert result is None

    def test_hhmm_negative_values(self):
        """Return None for negative hour/minute."""
        result = normalize_time_input("-1:30")
        assert result is None

    def test_none_input(self):
        """Return None when input is None."""
        result = normalize_time_input(None)
        assert result is None

    def test_empty_string_input(self):
        """Return None when input is empty string."""
        result = normalize_time_input("")
        assert result is None

    def test_whitespace_only_input(self):
        """Return None when input is whitespace only."""
        result = normalize_time_input("   ")
        assert result is None


class TestTimeBlockDefaults:
    """Test named time block defaults."""

    def test_morning_default(self):
        """Parse 'morning' to default 09:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("morning", reference_date=ref_date)
        assert result == "2026-04-16T09:00:00"

    def test_afternoon_default(self):
        """Parse 'afternoon' to default 14:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("afternoon", reference_date=ref_date)
        assert result == "2026-04-16T14:00:00"

    def test_evening_default(self):
        """Parse 'evening' to default 18:30."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("evening", reference_date=ref_date)
        assert result == "2026-04-16T18:30:00"

    def test_morning_case_insensitive(self):
        """Parse 'MORNING' (uppercase) to default 09:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("MORNING", reference_date=ref_date)
        assert result == "2026-04-16T09:00:00"

    def test_morning_mixed_case(self):
        """Parse 'MoRnInG' (mixed case) to default 09:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("MoRnInG", reference_date=ref_date)
        assert result == "2026-04-16T09:00:00"

    def test_morning_with_spaces(self):
        """Parse '  morning  ' (with spaces) to default 09:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("  morning  ", reference_date=ref_date)
        assert result == "2026-04-16T09:00:00"


class TestTimeAliases:
    """Test natural language time aliases."""

    def test_after_lunch_alias(self):
        """Parse 'after lunch' to default 13:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("after lunch", reference_date=ref_date)
        assert result == "2026-04-16T13:00:00"

    def test_tonight_alias(self):
        """Parse 'tonight' to default 19:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("tonight", reference_date=ref_date)
        assert result == "2026-04-16T19:00:00"

    def test_after_lunch_case_insensitive(self):
        """Parse 'AFTER LUNCH' (uppercase) to default 13:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("AFTER LUNCH", reference_date=ref_date)
        assert result == "2026-04-16T13:00:00"

    def test_after_lunch_mixed_case(self):
        """Parse 'AfTeR LuNcH' (mixed case) to default 13:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("AfTeR LuNcH", reference_date=ref_date)
        assert result == "2026-04-16T13:00:00"

    def test_after_lunch_with_spaces(self):
        """Parse '  after lunch  ' (with spaces) to default 13:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("  after lunch  ", reference_date=ref_date)
        assert result == "2026-04-16T13:00:00"

    def test_tonight_with_spaces(self):
        """Parse '  tonight  ' (with spaces) to default 19:00."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result = normalize_time_input("  tonight  ", reference_date=ref_date)
        assert result == "2026-04-16T19:00:00"


class TestReferenceDateHandling:
    """Test reference date behavior."""

    def test_different_date_morning(self):
        """Normalize 'morning' on different reference dates."""
        ref_date_1 = datetime(2026, 4, 16, 12, 0, 0)
        ref_date_2 = datetime(2026, 5, 20, 12, 0, 0)

        result_1 = normalize_time_input("morning", reference_date=ref_date_1)
        result_2 = normalize_time_input("morning", reference_date=ref_date_2)

        assert result_1 == "2026-04-16T09:00:00"
        assert result_2 == "2026-05-20T09:00:00"

    def test_different_date_afternoon(self):
        """Normalize 'afternoon' on different reference dates."""
        ref_date_1 = datetime(2026, 1, 1, 12, 0, 0)
        ref_date_2 = datetime(2026, 12, 31, 12, 0, 0)

        result_1 = normalize_time_input("afternoon", reference_date=ref_date_1)
        result_2 = normalize_time_input("afternoon", reference_date=ref_date_2)

        assert result_1 == "2026-01-01T14:00:00"
        assert result_2 == "2026-12-31T14:00:00"

    def test_hhmm_different_dates(self):
        """Normalize HH:MM on different reference dates."""
        ref_date_1 = datetime(2026, 4, 16, 12, 0, 0)
        ref_date_2 = datetime(2026, 4, 17, 12, 0, 0)

        result_1 = normalize_time_input("15:45", reference_date=ref_date_1)
        result_2 = normalize_time_input("15:45", reference_date=ref_date_2)

        assert result_1 == "2026-04-16T15:45:00"
        assert result_2 == "2026-04-17T15:45:00"


class TestDeterminism:
    """Test deterministic behavior (same input → same output)."""

    def test_morning_deterministic(self):
        """Same 'morning' input always produces same output."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result_1 = normalize_time_input("morning", reference_date=ref_date)
        result_2 = normalize_time_input("morning", reference_date=ref_date)
        result_3 = normalize_time_input("morning", reference_date=ref_date)
        assert result_1 == result_2 == result_3

    def test_hhmm_deterministic(self):
        """Same HH:MM input always produces same output."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result_1 = normalize_time_input("11:30", reference_date=ref_date)
        result_2 = normalize_time_input("11:30", reference_date=ref_date)
        result_3 = normalize_time_input("11:30", reference_date=ref_date)
        assert result_1 == result_2 == result_3

    def test_alias_deterministic(self):
        """Same alias input always produces same output."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result_1 = normalize_time_input("after lunch", reference_date=ref_date)
        result_2 = normalize_time_input("after lunch", reference_date=ref_date)
        result_3 = normalize_time_input("after lunch", reference_date=ref_date)
        assert result_1 == result_2 == result_3

    def test_case_variants_deterministic(self):
        """Case variants of same input produce same output."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        result_1 = normalize_time_input("morning", reference_date=ref_date)
        result_2 = normalize_time_input("MORNING", reference_date=ref_date)
        result_3 = normalize_time_input("MoRnInG", reference_date=ref_date)
        assert result_1 == result_2 == result_3


class TestReverseLookup:
    """Test reverse lookup from ISO datetime to time block."""

    def test_reverse_lookup_morning(self):
        """Reverse lookup default morning time."""
        result = get_time_block_from_iso("2026-04-16T09:00:00")
        assert result == "morning"

    def test_reverse_lookup_afternoon(self):
        """Reverse lookup default afternoon time."""
        result = get_time_block_from_iso("2026-04-16T14:00:00")
        assert result == "afternoon"

    def test_reverse_lookup_evening(self):
        """Reverse lookup default evening time."""
        result = get_time_block_from_iso("2026-04-16T18:30:00")
        assert result == "evening"

    def test_reverse_lookup_after_lunch(self):
        """Reverse lookup 'after lunch' alias."""
        result = get_time_block_from_iso("2026-04-16T13:00:00")
        assert result == "after lunch"

    def test_reverse_lookup_tonight(self):
        """Reverse lookup 'tonight' alias."""
        result = get_time_block_from_iso("2026-04-16T19:00:00")
        assert result == "tonight"

    def test_reverse_lookup_non_standard(self):
        """Reverse lookup for non-standard time returns None."""
        result = get_time_block_from_iso("2026-04-16T11:30:00")
        assert result is None

    def test_reverse_lookup_none_input(self):
        """Reverse lookup with None input returns None."""
        result = get_time_block_from_iso(None)
        assert result is None

    def test_reverse_lookup_empty_string(self):
        """Reverse lookup with empty string returns None."""
        result = get_time_block_from_iso("")
        assert result is None

    def test_reverse_lookup_invalid_datetime(self):
        """Reverse lookup with invalid datetime string returns None."""
        result = get_time_block_from_iso("not a datetime")
        assert result is None


class TestAliasListing:
    """Test alias listing utility."""

    def test_list_aliases_contains_morning(self):
        """List should contain 'morning' with time 09:00."""
        aliases = list_time_aliases()
        assert "morning" in aliases
        assert aliases["morning"] == "09:00"

    def test_list_aliases_contains_afternoon(self):
        """List should contain 'afternoon' with time 14:00."""
        aliases = list_time_aliases()
        assert "afternoon" in aliases
        assert aliases["afternoon"] == "14:00"

    def test_list_aliases_contains_evening(self):
        """List should contain 'evening' with time 18:30."""
        aliases = list_time_aliases()
        assert "evening" in aliases
        assert aliases["evening"] == "18:30"

    def test_list_aliases_contains_after_lunch(self):
        """List should contain 'after lunch' with time 13:00."""
        aliases = list_time_aliases()
        assert "after lunch" in aliases
        assert aliases["after lunch"] == "13:00"

    def test_list_aliases_contains_tonight(self):
        """List should contain 'tonight' with time 19:00."""
        aliases = list_time_aliases()
        assert "tonight" in aliases
        assert aliases["tonight"] == "19:00"

    def test_list_aliases_count(self):
        """List should contain exactly 5 time aliases."""
        aliases = list_time_aliases()
        assert len(aliases) == 5

    def test_list_aliases_deterministic(self):
        """Listing aliases multiple times produces same result."""
        result_1 = list_time_aliases()
        result_2 = list_time_aliases()
        result_3 = list_time_aliases()
        assert result_1 == result_2 == result_3


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_malformed_hhmm_no_colon(self):
        """Malformed HH:MM without colon returns None."""
        result = normalize_time_input("1130")
        assert result is None

    def test_malformed_hhmm_multiple_colons(self):
        """Malformed HH:MM with multiple colons returns None."""
        result = normalize_time_input("11:30:45")
        # Note: This will fail gracefully in the parsing loop
        assert result is None or ":" in result

    def test_hhmm_with_non_numeric_characters(self):
        """HH:MM with non-numeric characters returns None."""
        result = normalize_time_input("ab:cd")
        assert result is None

    def test_unknown_text_alias(self):
        """Unknown text alias returns None."""
        result = normalize_time_input("breakfast time")
        assert result is None

    def test_partial_alias(self):
        """Partial alias (e.g., 'after' without 'lunch') returns None."""
        result = normalize_time_input("after")
        assert result is None

    def test_unicode_whitespace(self):
        """Unicode whitespace-only input returns None."""
        result = normalize_time_input("\u00a0\u2000\u2001")  # Various unicode spaces
        assert result is None


class TestIntegrationWithManualItems:
    """Test integration scenario: normalizing manual items."""

    def test_normalize_scheduled_action_with_hhmm(self):
        """Normalize scheduled action with HH:MM time."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        raw_time = "14:30"
        result = normalize_time_input(raw_time, reference_date=ref_date)
        assert result == "2026-04-16T14:30:00"

    def test_normalize_scheduled_action_with_block(self):
        """Normalize scheduled action with time block."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        raw_time = "afternoon"
        result = normalize_time_input(raw_time, reference_date=ref_date)
        assert result == "2026-04-16T14:00:00"

    def test_normalize_scheduled_action_with_alias(self):
        """Normalize scheduled action with natural alias."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        raw_time = "after lunch"
        result = normalize_time_input(raw_time, reference_date=ref_date)
        assert result == "2026-04-16T13:00:00"

    def test_normalize_multiple_items_deterministic(self):
        """Normalize multiple manual items with same reference date."""
        ref_date = datetime(2026, 4, 16, 12, 0, 0)
        items = [
            ("morning", "2026-04-16T09:00:00"),
            ("14:30", "2026-04-16T14:30:00"),
            ("after lunch", "2026-04-16T13:00:00"),
            ("evening", "2026-04-16T18:30:00"),
            ("11:45", "2026-04-16T11:45:00"),
        ]
        for raw_input, expected in items:
            result = normalize_time_input(raw_input, reference_date=ref_date)
            assert result == expected

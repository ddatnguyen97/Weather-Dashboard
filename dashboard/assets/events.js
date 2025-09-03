(function () {
  const SELECTOR = "#global-date-picker input";

  function pushEvent(input) {
    const value = input.value;
    if (!value || input._lastPushed === value) return;

    input._lastPushed = value;

    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
      event: "click_date_filter_btn",
      filter_value: value,
      debug_mode: true,
    });

    console.log("Event pushed:", value);
  }

  function initTracking(input) {
    // Prefer "change" for date pickers (fires once per selection)
    input.addEventListener("change", () => pushEvent(input));

    // As a fallback, observer in case the picker updates value silently
    new MutationObserver(() => pushEvent(input)).observe(input, {
      attributes: true,
      attributeFilter: ["value"],
    });

    console.log("Date picker tracking initialized");
  }

  function waitForInput() {
    const input = document.querySelector(SELECTOR);
    if (input) {
      initTracking(input);
    } else {
      setTimeout(waitForInput, 300);
    }
  }

  document.addEventListener("DOMContentLoaded", waitForInput);
})();

window.dataLayer = window.dataLayer || [];
// function gtag() {
//   dataLayer.push(arguments);
// }

// window.addEventListener("load", function () {
//   gtag("js", new Date());
//   gtag("config", "G-SBGP0H0LEN");
// });

function pushDateFilterEvent(filterValue) {
  console.log("Pushing to dataLayer:", filterValue);
  dataLayer.push({
    event: "click_date_filter_btn",
    filter_value: filterValue,
  });
}

function observeDateInputChange() {
  const dateInput = document.querySelector("#global-date-picker input");

  if (!dateInput) {
    console.warn("Date input not found, retrying...");
    setTimeout(observeDateInputChange, 500);
    return;
  }

  let lastValue = dateInput.value;

  ["change", "input"].forEach((evt) =>
    dateInput.addEventListener(evt, () => {
      if (dateInput.value !== lastValue) {
        lastValue = dateInput.value;
        console.log("Date changed via event:", lastValue);
        pushDateFilterEvent(lastValue);
      }
    })
  );

  const observer = new MutationObserver(() => {
    if (dateInput.value !== lastValue) {
      lastValue = dateInput.value;
      console.log("Date changed via mutation:", lastValue);
      pushDateFilterEvent(lastValue);
    }
  });

  observer.observe(dateInput, {
    attributes: true,
    attributeFilter: ["value"],
  });

  console.log("Observer + event listeners attached");
}

document.addEventListener("DOMContentLoaded", observeDateInputChange);

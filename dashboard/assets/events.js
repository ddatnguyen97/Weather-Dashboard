window.dataLayer = window.dataLayer || [];
function gtag() {
  dataLayer.push(arguments);
}

gtag("js", new Date());
gtag("config", "G-SBGP0H0LEN");

function pushDateFilterEvent(filterValue) {
  dataLayer.push({
    event: "click_date_filter_btn",
    filter_value: filterValue,
  });
}

// document.addEventListener("DOMContentLoaded", function () {
//   const dateInput = document.querySelector("#global-date-picker input");

//   if (dateInput) {
//     dateInput.addEventListener("change", function () {
//       console.log("Date selected:", dateInput.value);
//       pushDateFilterEvent(dateInput.value);
//     });
//   } else {
//     console.warn("Date input not found inside #global-date-picker");
//   }
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

  // Fallback: listen for normal events
  ["change", "input"].forEach((evt) =>
    dateInput.addEventListener(evt, () => {
      if (dateInput.value !== lastValue) {
        lastValue = dateInput.value;
        console.log("Date changed via event:", lastValue);
        pushDateFilterEvent(lastValue);
      }
    })
  );

  // Observer: detect framework-driven attribute changes
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

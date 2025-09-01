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

document.addEventListener("DOMContentLoaded", function () {
  const dateInput = document.querySelector("#global-date-picker input");

  if (dateInput) {
    dateInput.addEventListener("change", function () {
      console.log("Date selected:", dateInput.value);
      pushDateFilterEvent(dateInput.value);
    });
  } else {
    console.warn("Date input not found inside #global-date-picker");
  }
});

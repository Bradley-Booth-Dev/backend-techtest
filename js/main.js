const axios = require("axios");

const supportersEndpoint = "https://www.few-far.co/api/techtest/v1/supporters";
const donationsEndpoint = "https://www.few-far.co/api/techtest/v1/donations";

// Function to fetch supporter data
async function fetchSupporters() {
  try {
    const response = await axios.get(supportersEndpoint);
    console.log(response.data);
    return response.data;
  } catch (error) {
    console.error("Error fetching supporter data:", error);
    throw error;
  }
}

// Function to fetch donation data
async function fetchDonations() {
  try {
    const response = await axios.get(donationsEndpoint);
    return response.data;
  } catch (error) {
    console.error("Error fetching donation data:", error);
    throw error;
  }
}

fetchSupporters();

// Function to calculate total donations per supporter
function calculateTotalDonations(supporters, donations) {
  const supporterDonations = {};

  // Initialize supporter donation totals
  supporters.forEach((supporter) => {
    supporterDonations[supporter.id] = 0;
  });

  // Calculate total donations for each supporter
  donations.forEach((donation) => {
    const supporterId = donation.supporter_id;
    const donationAmount = donation.amount;
    supporterDonations[supporterId] += donationAmount;
  });

  return supporterDonations;
}

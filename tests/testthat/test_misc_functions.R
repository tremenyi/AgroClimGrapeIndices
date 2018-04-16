context("Helper Functions")


#' Austral Season Months:
#' 01 Jul  02 Aug  03 Sep  04 Oct  05 Nov  06 Dec
#' 07 Jan  08 Feb  09 Mar  10 Apr  11 May  12 Jun
all_months <- 1:12

test_that("is_Oct_to_Apr is computed correctly", {
  expected_OA <- c(
    FALSE, FALSE, FALSE, TRUE, TRUE, TRUE,
    TRUE, TRUE, TRUE, TRUE, FALSE, FALSE
  )

  expect_equal(is_Oct_to_Apr(all_months), expected_OA)
})


test_that("is_Sep_to_Apr is computed correctly", {
  expected_SA <- c(
    FALSE, FALSE, TRUE, TRUE, TRUE, TRUE,
    TRUE, TRUE, TRUE, TRUE, FALSE, FALSE
  )

  expect_equal(is_Sep_to_Apr(all_months), expected_SA)
})


test_that("is_Mar_to_Apr is computed correctly", {
  expected_MA <- c(
    FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
    FALSE, FALSE, TRUE, TRUE, FALSE, FALSE
  )

  expect_equal(is_Mar_to_Apr(all_months), expected_MA)
})


test_that("Last date of temperature instance is found", {

  # Data --------
  test_df <- data.frame(
    date = seq(as.Date("1961-07-01"), as.Date("1961-07-31"), "days"),
    tasmin = c(
      # 1-10
      2.17, 7.35, 6.28, 9.01, 6.37, 6.55, 3.01, 6.18, 5.21, 5.62,
      # 11-20
      2.15, 2.25, 5.33, 6.62, 4.83, 3.31, 3.88, 3.19, -0.38, 0.21,
      # 21-30
      -1.65, -0.49, 5.17, 5.59, 4.04, 4.08, 6.23, 1.57, 6.83, 5.33,
      # 31
      5.05)
  )

  # Test --------
  expect_equal(find_last_date_under(test_df$date, test_df$tasmin, 0), as.Date("1961-07-22"))
  expect_equal(find_last_date_under(test_df$date, test_df$tasmin, 2), as.Date("1961-07-28"))
  expect_equal(find_last_date_under(test_df$date, test_df$tasmin, -20), NA)
})


test_that("Downy Mildew Pressure is calculated correctly", {

  ## context: test rainfall -- duration definitely longer than 10 hours
  #
  expect_equal(downy_mildew_pressure_index(rain = 9,  T_max = 20, T_min = 8, T_thresh = 10), FALSE)
  expect_equal(downy_mildew_pressure_index(rain = 10, T_max = 20, T_min = 8, T_thresh = 10), TRUE)

  ## context: test duration -- all cases where rainfall criteria is met
  # examples are pre calculated manually (Pythagoras...)
  #
  expect_equal(downy_mildew_pressure_index(rain = 11, T_max = 20,   T_min = 10,   T_thresh = 10), TRUE)   # 24
  expect_equal(downy_mildew_pressure_index(rain = 11, T_max = 22,   T_min = 6,    T_thresh = 10), TRUE)   # 18
  expect_equal(downy_mildew_pressure_index(rain = 11, T_max = 22,   T_min = -6.8, T_thresh = 10), TRUE)   # 10
  expect_equal(downy_mildew_pressure_index(rain = 11, T_max = 22.5, T_min = -6.8, T_thresh = 10), TRUE)   # >10 just (nudge T_max higher)
  expect_equal(downy_mildew_pressure_index(rain = 11, T_max = 21.5, T_min = -6.8, T_thresh = 10), FALSE)  # <10 just (nudge T_max lower)
  expect_equal(downy_mildew_pressure_index(rain = 11, T_max = 13,   T_min = 4,    T_thresh = 10), FALSE)  # 8
})

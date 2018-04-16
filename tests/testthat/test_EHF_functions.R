context("EHF")

# common data
DMT_threshold <- 19.5
temps <- c(20.48, 17.81, 19.06, 16.40, 16.34, 20.79, 18.82, 16.28, 18.25, 19.56,
           15.57, 18.60, 17.22, 19.12, 19.02, 15.58, 20.88, 15.62, 15.27, 20.52,
           19.13, 18.46, 16.80, 15.26, 20.71, 20.74, 15.74, 16.63, 18.88, 18.67,
           15.32, 16.83, 17.86, 19.04, 19.62, 19.14, 17.99, 19.36, 16.04, 18.41,
           19.02, 20.77, 17.99, 20.74, 20.41, 20.80, 20.40, 18.06, 16.32, 18.25)

# pre calculated answers from spreadsheet
# just summed the rows, instead of inserting complete lists
pre_calc_EHI_sig <- -58.6166666667
pre_calc_EHI_accl <- 13.8603333333
pre_calc_EHF <- -8.6549955556


# let
test_EHI_sig <- calc_EHI_sig(temps, DMT_threshold)

test_EHI_accl <- calc_EHI_accl(temps)


test_that("EHI_sig is computed correctly", {
  expect_equal(sum(test_EHI_sig, na.rm = TRUE), pre_calc_EHI_sig)
})


test_that("EHI_accl is computed correctly", {
  expect_equal(sum(test_EHI_accl, na.rm = TRUE), pre_calc_EHI_accl)
})


test_that("EHF is computed correctly", {
  test_EHF <- calc_EHF(test_EHI_sig, test_EHI_accl)

  expect_equal(sum(test_EHF, na.rm = TRUE), pre_calc_EHF)
})


test_that("EHF events are detected correctly", {
  EHF_gt_zero <- c(FALSE, FALSE, FALSE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, FALSE,
                   TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE)

  # context("with duration of 3")
  params_3 <- c(min_duration = 3)
  exp_result_3 <- c(FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, FALSE, FALSE,
                   FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE)

  expect_equal(heatwave_detect_ehf(EHF_gt_zero, params_3), exp_result_3)

  # context("with duration of 4")
  params_4 <- c(min_duration = 4)
  exp_result_4 <- c(FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
                    FALSE, FALSE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE)

  expect_equal(heatwave_detect_ehf(EHF_gt_zero, params_4), exp_result_4)
})

def run_pipeline() -> None:
    import extract as step_a
    import validate_input as step_b
    import quality_analysis as step_c
    import clean as step_d
    import transform as step_e
    import validate_output as step_f
    import dimensional_model as step_g
    import load_dw as step_h
    import analysis as step_i
    step_a.main()
    step_b.main()
    step_c.main()
    step_d.main()
    step_e.main()
    step_f.main()
    step_g.main()
    step_h.main()
    step_i.main()

if __name__ == "__main__":
    run_pipeline()
